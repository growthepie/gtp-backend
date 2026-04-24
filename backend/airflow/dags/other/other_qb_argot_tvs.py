from datetime import datetime, timedelta
from airflow.sdk import dag, task
from pendulum import now
from src.misc.airflow_utils import alert_via_webhook, claude_fix_on_failure

# configured to run monthly on the 2nd
# will not backfill automatically

@dag(
    default_args={
        'owner': 'lorenz',
        'retries': 2,
        'email_on_failure': False,
        'retry_delay': timedelta(minutes=5),
        'on_failure_callback': [alert_via_webhook, claude_fix_on_failure]
    },
    dag_id='other_qb_argot_tvs',
    description='Top TVS contracts which are verified.',
    tags=['other', 'monthly'],
    start_date=datetime(2026, 4, 1),
    schedule='0 11 2 * *'  # 2nd of every month at 11:00 UTC
)
def run_dag():

    @task()
    def ensure_dune_table_exists():
        """Create the l2beat_tokens Dune table if it does not already exist."""
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        db_connector = DbConnector()
        ad = AdapterDune({'api_key': os.getenv("DUNE_API")}, db_connector)
        ad.create_table(
            table_name="l2beat_tokens",
            description="All tokens tracked by L2Beat across supported networks",
            schema=[
                {"name": "network",               "type": "varchar"},
                {"name": "symbol",                "type": "varchar"},
                {"name": "address",               "type": "varchar"},
                {"name": "coingecko_id",          "type": "varchar"},
                {"name": "category",              "type": "varchar"},
                {"name": "supply",                "type": "varchar"},
                {"name": "source",                "type": "varchar"},
                {"name": "bridge_names",          "type": "varchar"},
                {"name": "bridge_slugs",          "type": "varchar"},
                {"name": "deployment_timestamp",  "type": "bigint"},
                {"name": "exclude_from_total",    "type": "boolean"},
            ]
        )

    @task()
    def scrape_and_upload():
        """Fetch the L2Beat tokens.jsonc, flatten it into a DataFrame, and upload to Dune."""
        import re
        import json
        import os
        import requests
        import pandas as pd
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        TOKENS_URL = (
            "https://raw.githubusercontent.com/l2beat/l2beat/"
            "f63bf02c80242e767fb36b44b06180ac5b9a83ff/"
            "packages/config/src/tokens/tokens.jsonc"
        )

        # --- Fetch ---
        print(f"Fetching tokens from {TOKENS_URL}")
        response = requests.get(TOKENS_URL, timeout=30)
        response.raise_for_status()
        raw = response.text

        # --- Strip JSONC single-line comments before parsing ---
        clean = re.sub(r'//[^\n]*', '', raw)
        data = json.loads(clean)
        print(f"Parsed {sum(len(v) for v in data.values())} tokens across {len(data)} networks.")

        # --- Flatten into rows ---
        rows = []
        for network, tokens in data.items():
            for token in tokens:
                bridged = token.get('bridgedUsing') or {}
                bridges = bridged.get('bridges') or []
                bridge_names = '|'.join(b.get('name', '') for b in bridges) or None
                bridge_slugs = '|'.join(b.get('slug', '') for b in bridges if b.get('slug')) or None

                rows.append({
                    'network':              network,
                    'symbol':               token.get('symbol'),
                    'address':              token.get('address'),
                    'coingecko_id':         token.get('coingeckoId'),
                    'category':             token.get('category'),
                    'supply':               token.get('supply'),
                    'source':               token.get('source'),
                    'bridge_names':         bridge_names,
                    'bridge_slugs':         bridge_slugs,
                    'deployment_timestamp': token.get('deploymentTimestamp'),
                    'exclude_from_total':   token.get('excludeFromTotal'),
                })

        # we exclude the following, otherwise they would skew the TVL for certain dates (LOWERCASE!)

        EXCLUDED_ADDRESSES = {
            '0x7cf9a80db3b29ee8efe3710aadb7b95270572d47', # NIL
            '0x090185f2135308bad17527004364ebcc2d37e5f6', # Linea
            '0x1789e0043623282d5dcc7f213d703c6d8bafbb04', # SPELL
            '0x62b9c7356a2dc64a1969e19c23e4f579f9810aa7', # cvxCRV
            '0xa2085073878152ac3090ea13d1e41bd69e60dc99', # ELG
            '0xfb5c6815ca3ac72ce9f5006869ae67f18bf77006', # PSTAKE
            '0xa0b73e1ff0b80914ab6fe0444e65848c4c34450b', # CRO
            '0x0ab87046fbb341d058f17cbc4c1133f25a20a52f', # gOHM
            '0x0d292b4a8fcfed626dc73242cec1da0503b891fd', # TRN
            '0x104f3152d8ebfc3f679392977356962ff36566ac', # PORTX
        }


        df = pd.DataFrame(rows)
        df = df[df['address'].notna()]
        df = df[~df['address'].str.lower().isin(EXCLUDED_ADDRESSES)]
        # Cast to nullable Int64 so NaN rows stay null instead of becoming 1234.0 floats
        df['deployment_timestamp'] = df['deployment_timestamp'].astype('Int64')
        print(f"Built DataFrame with {len(df)} valid addresses, columns: {list(df.columns)}")

        # --- Upload to Dune ---
        db_connector = DbConnector()
        ad = AdapterDune({'api_key': os.getenv("DUNE_API")}, db_connector)
        ad.upload_to_table("l2beat_tokens", df)

        # --- Upload token metadata JSON to S3 ---
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan

        df_eth = df[df['network'] == 'ethereum']
        symbols = sorted(df_eth['symbol'].dropna().unique().tolist() + ['ETH'])
        meta = {
            'token_count': len(df_eth) + 1,
            'symbols': symbols,
        }
        meta = fix_dict_nan(meta, 'tvs_token_metadata', send_notification=False)

        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")
        upload_json_to_cf_s3(s3_bucket, 'v1/quick-bites/argot/tvs_token_metadata', meta, cf_distribution_id, invalidate=False)
        print(f"Uploaded token metadata to S3: token_count={len(df_eth)}, unique symbols={len(symbols)}")

    @task()
    def clear_dune_table():
        """Clear all rows from the l2beat_tokens Dune table before re-uploading."""
        import os
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune

        db_connector = DbConnector()
        ad = AdapterDune({'api_key': os.getenv("DUNE_API")}, db_connector)
        ad.clear_table("l2beat_tokens")

    @task()
    def get_data_agg_table():
        """Fetch Dune query 6997383 (Argot contract TVS) and upsert aggregations into fact_kpis."""
        import os
        import pandas as pd
        from datetime import datetime, timezone
        from src.db_connector import DbConnector
        from src.adapters.adapter_dune import AdapterDune
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan

        now = datetime.now(timezone.utc)
        snapshot_date = now.strftime("%Y-%m-01")

        adapter_params = {'api_key': os.getenv("DUNE_API")}
        load_params = {
            'queries': [
                {
                    'name': 'argot-contract-tvs',
                    'query_id': 6997383,
                    'params': {'snapshot': snapshot_date}
                }
            ]
        }
        db_connector = DbConnector()
        ad = AdapterDune(adapter_params, db_connector)
        df = ad.extract(load_params)
        print(f"Fetched {len(df)} rows from Dune query 6997383.")

        # dune returns two columns
        df = df[['address', 'total_balance_usd']]

        # left join sourcify data to df based on address
        sourcify_data = db_connector.execute_query("""
            WITH sourcify_attestations AS (
                SELECT DISTINCT ON (chain_id, recipient)
                    chain_id,
                    recipient,
                    tags_json->>'code_compiler' AS code_compiler,
                    tags_json->>'code_language' AS code_language
                FROM attestations
                WHERE
                    attester = decode('8DBAE854E53AEDFC3B8D78F7C1ADD53935939192', 'hex')
                    AND revoked = false
                    AND chain_id = 'eip155:1'
                ORDER BY chain_id, recipient, time DESC
            )
            SELECT
                a.recipient AS address,
                a.code_language AS compiler,
                a.code_compiler AS version,
                substring(split_part(a.code_compiler, '-', 2) FROM '^\d+\.\d+') AS version_short
            FROM sourcify_attestations a
            INNER JOIN sys_main_conf mc ON a.chain_id = mc.caip2
        """, load_df=True)
        df = df.merge(sourcify_data, on='address', how='left')

        rows = []

        # --- compiler aggregation → sourcify_top1000_compiler_{compiler}_count/usd_total ---
        df['compiler'] = df['compiler'].fillna('unknown')
        df['compiler'] = df['compiler'].replace('solc', 'solidity')

        # Dune fallback lookup for addresses not found in sourcify
        unknown = df[df['compiler'] == 'unknown']['address'].to_list()
        if unknown:
            unknown_str = "|".join(unknown)
            dune_lookup = ad.extract({
                'queries': [
                    {
                        'name': 'argot-address-compiler-lookup',
                        'query_id': 7357764,
                        'params': {'addresses': unknown_str}
                    }
                ]
            })
            dune_found = dune_lookup[dune_lookup['compiler'] != '<nil>'][['address', 'compiler']]
            if not dune_found.empty:
                dune_map = dune_found.drop_duplicates(subset='address').set_index('address')['compiler']
                df.loc[df['compiler'] == 'unknown', 'compiler'] = (
                    df.loc[df['compiler'] == 'unknown', 'address'].map(dune_map).fillna('unknown')
                )
                print(f"Dune lookup resolved {len(dune_found)} previously unknown addresses.")

        df_compiler = df.groupby('compiler').agg(
            total_usd=('total_balance_usd', 'sum'),
            count=('total_balance_usd', 'count')
        ).reset_index()
        for _, row in df_compiler.iterrows():
            slug = row['compiler'].lower().replace(' ', '_')
            rows.append({'metric_key': f'cmp_{slug}_usd', 'origin_key': 'ethereum', 'date': snapshot_date, 'value': row['total_usd']})
            rows.append({'metric_key': f'cmp_{slug}_ct', 'origin_key': 'ethereum', 'date': snapshot_date, 'value': row['count']})

        # --- language aggregation → sourcify_top1000_{compiler}_{version_short}_count/usd_total ---
        df_language = df.groupby(['compiler', 'version_short']).agg(
            total_usd=('total_balance_usd', 'sum'),
            count=('total_balance_usd', 'count')
        ).reset_index()
        for _, row in df_language.iterrows():
            slug = f"{row['compiler']}_{row['version_short']}".lower().replace(' ', '_')
            rows.append({'metric_key': f'cmp_{slug}_usd', 'origin_key': 'ethereum', 'date': snapshot_date, 'value': row['total_usd']})
            rows.append({'metric_key': f'cmp_{slug}_ct', 'origin_key': 'ethereum', 'date': snapshot_date, 'value': row['count']})

        df_kpis = pd.DataFrame(rows)
        df_kpis['date'] = pd.to_datetime(df_kpis['date']).dt.date
        df_kpis['value'] = df_kpis['value'].astype(float)
        df_kpis = df_kpis.set_index(['metric_key', 'origin_key', 'date'])

        upserted = db_connector.upsert_table('fact_kpis', df_kpis)
        print(f"Upserted {upserted} rows into fact_kpis.")

        # --- enrich df with owner_project & contract_name ---
        gtp_labels = db_connector.execute_query("""
            SELECT
                '0x' || encode(address, 'hex') AS address,
                contract_name AS name,
                owner_project,
                d.display_name
            FROM public.vw_oli_label_pool_gold_pivoted_v2
            LEFT JOIN public.oli_oss_directory d ON d.name = owner_project
            WHERE caip2 = 'eip155:1'
        """, load_df=True)
        gtp_labels = gtp_labels.rename(columns={'tag_value': 'owner_project'})
        df = df.merge(gtp_labels, on='address', how='left')

        # --- table JSON upload ---
        cols = ['address', 'total_balance_usd', 'compiler', 'version', 'name', 'owner_project', 'display_name']
        table_dict = {}
        table_dict["data"] = {
            "types": cols,
            "values": df[cols].values.tolist()
        }
        table_dict = fix_dict_nan(table_dict, 'argot_table', send_notification=False)

        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")
        upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/argot/table_{snapshot_date}', table_dict, cf_distribution_id, invalidate=False)
        upload_json_to_cf_s3(s3_bucket, f'v1/quick-bites/argot/table_latest', table_dict, cf_distribution_id, invalidate=False)
        print(f"Uploaded to S3: v1/quick-bites/argot/table_{snapshot_date}")

    @task()
    def create_solidity_jsons():
        """Query fact_kpis for Solidity compiler breakdowns and upload JSONs to S3."""
        import os
        import pandas as pd
        from src.db_connector import DbConnector
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan

        db_connector = DbConnector()

        QUERY_TVS = """
            SELECT
                substring(metric_key FROM 'cmp_solidity_(\\d+\\.\\d+)_usd') AS solc_major_version,
                "date",
                SUM(value) AS value
            FROM public.fact_kpis
            WHERE
                metric_key NOT IN ('cmp_solidity_usd','cmp_vyper_usd','cmp_unknown_usd','cmp_solidity_ct','cmp_vyper_ct','cmp_unknown_ct')
                AND metric_key LIKE 'cmp_solidity_%%'
                AND metric_key LIKE '%%_usd'
                AND origin_key = 'ethereum'
                AND substring(metric_key FROM 'cmp_solidity_(\\d+\\.\\d+)_usd') IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 2 DESC
        """

        QUERY_CT = """
            SELECT
                substring(metric_key FROM 'cmp_solidity_(\\d+\\.\\d+)_ct') AS solc_major_version,
                "date",
                SUM(value) AS value
            FROM public.fact_kpis
            WHERE
                metric_key NOT IN ('cmp_solidity_usd','cmp_vyper_usd','cmp_unknown_usd','cmp_solidity_ct','cmp_vyper_ct','cmp_unknown_ct')
                AND metric_key LIKE 'cmp_solidity_%%'
                AND metric_key LIKE '%%_ct'
                AND origin_key = 'ethereum'
                AND substring(metric_key FROM 'cmp_solidity_(\\d+\\.\\d+)_ct') IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 2 DESC
        """

        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        for query, s3_key, label in [
            (QUERY_TVS, 'v1/quick-bites/argot/solc_tvs_timeseries', 'ethereum_solc_tvs'),
            (QUERY_CT,  'v1/quick-bites/argot/solc_ct_timeseries',  'ethereum_solc_ct'),
        ]:
            df = pd.read_sql(query, db_connector.engine)
            df['date'] = pd.to_datetime(df['date']).astype('datetime64[ms]').astype('int64')
            df = df.pivot(index='date', columns='solc_major_version', values='value').reset_index().sort_values('date')
            df.columns.name = None
            cols = df.columns.tolist()
            payload = {'data': {'types': cols, 'values': df.values.tolist()}}
            payload = fix_dict_nan(payload, label, send_notification=False)
            upload_json_to_cf_s3(s3_bucket, s3_key, payload, cf_distribution_id, invalidate=False)
            print(f"Uploaded {len(df)} rows to S3: {s3_key}")

    @task()
    def create_vyper_jsons():
        """Query fact_kpis for Vyper compiler breakdowns and upload JSONs to S3."""
        import os
        import pandas as pd
        from src.db_connector import DbConnector
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan

        db_connector = DbConnector()

        QUERY_TVS = """
            SELECT
                substring(metric_key FROM 'cmp_vyper_(\\d+\\.\\d+)_usd') AS vyper_major_version,
                "date",
                SUM(value) AS value
            FROM public.fact_kpis
            WHERE
                metric_key NOT IN ('cmp_vyper_usd','cmp_unknown_usd','cmp_vyper_ct','cmp_unknown_ct')
                AND metric_key LIKE 'cmp_vyper_%%'
                AND metric_key LIKE '%%_usd'
                AND origin_key = 'ethereum'
                AND substring(metric_key FROM 'cmp_vyper_(\\d+\\.\\d+)_usd') IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 2 DESC
        """

        QUERY_CT = """
            SELECT
                substring(metric_key FROM 'cmp_vyper_(\\d+\\.\\d+)_ct') AS vyper_major_version,
                "date",
                SUM(value) AS value
            FROM public.fact_kpis
            WHERE
                metric_key NOT IN ('cmp_vyper_usd','cmp_unknown_usd','cmp_vyper_ct','cmp_unknown_ct')
                AND metric_key LIKE 'cmp_vyper_%%'
                AND metric_key LIKE '%%_ct'
                AND origin_key = 'ethereum'
                AND substring(metric_key FROM 'cmp_vyper_(\\d+\\.\\d+)_ct') IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 2 DESC
        """

        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        for query, s3_key, label in [
            (QUERY_TVS, 'v1/quick-bites/argot/vyper_tvs_timeseries', 'ethereum_vyper_tvs'),
            (QUERY_CT,  'v1/quick-bites/argot/vyper_ct_timeseries',  'ethereum_vyper_ct'),
        ]:
            df = pd.read_sql(query, db_connector.engine)
            df['date'] = pd.to_datetime(df['date']).astype('datetime64[ms]').astype('int64')
            df = df.pivot(index='date', columns='vyper_major_version', values='value').reset_index().sort_values('date')
            df.columns.name = None
            cols = df.columns.tolist()
            payload = {'data': {'types': cols, 'values': df.values.tolist()}}
            payload = fix_dict_nan(payload, label, send_notification=False)
            upload_json_to_cf_s3(s3_bucket, s3_key, payload, cf_distribution_id, invalidate=False)
            print(f"Uploaded {len(df)} rows to S3: {s3_key}")

    @task()
    def create_compiler_jsons():
        """Query fact_kpis for top-level compiler totals (solc/vyper/unknown) and upload JSONs to S3."""
        import os
        import pandas as pd
        from src.db_connector import DbConnector
        from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan

        db_connector = DbConnector()

        QUERY_CT = """
            SELECT
                "date",
                substring(metric_key FROM 'cmp_(.+)_ct') AS compiler,
                value
            FROM public.fact_kpis
            WHERE
                metric_key IN ('cmp_solidity_ct','cmp_vyper_ct','cmp_unknown_ct')
                AND origin_key = 'ethereum'
                AND "date" >= '2018-01-01'
            ORDER BY 1 DESC
        """

        QUERY_TVS = """
            SELECT
                substring(metric_key FROM 'cmp_(.+)_usd') AS compiler,
                "date",
                value
            FROM public.fact_kpis
            WHERE
                metric_key IN ('cmp_solidity_usd','cmp_vyper_usd','cmp_unknown_usd')
                AND origin_key = 'ethereum'
                AND "date" >= '2018-01-01'
            ORDER BY 2 DESC
        """

        s3_bucket = os.getenv("S3_CF_BUCKET")
        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")

        for query, s3_key, label in [
            (QUERY_CT,  'v1/quick-bites/argot/compiler_ct_timeseries',  'ethereum_compiler_ct'),
            (QUERY_TVS, 'v1/quick-bites/argot/compiler_tvs_timeseries', 'ethereum_compiler_tvs'),
        ]:
            df = pd.read_sql(query, db_connector.engine)
            df['date'] = pd.to_datetime(df['date']).astype('datetime64[ms]').astype('int64')
            df = df.pivot(index='date', columns='compiler', values='value').reset_index().sort_values('date')
            df.columns.name = None
            cols = df.columns.tolist()
            payload = {'data': {'types': cols, 'values': df.values.tolist()}}
            payload = fix_dict_nan(payload, label, send_notification=False)
            upload_json_to_cf_s3(s3_bucket, s3_key, payload, cf_distribution_id, invalidate=False)
            print(f"Uploaded {len(df)} rows to S3: {s3_key}")

    @task()
    def invalidate_cf_cache():
        """Invalidate the whole argot quick-bite folder once all uploads are done."""
        import os
        from src.misc.helper_functions import empty_cloudfront_cache

        cf_distribution_id = os.getenv("CF_DISTRIBUTION_ID")
        empty_cloudfront_cache(cf_distribution_id, '/v1/quick-bites/argot/*')


    ensure_dune_table_exists() >> clear_dune_table() >> scrape_and_upload() >> get_data_agg_table() >> [create_solidity_jsons(), create_vyper_jsons(), create_compiler_jsons()] >> invalidate_cf_cache()


run_dag()
