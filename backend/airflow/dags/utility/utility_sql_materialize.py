from datetime import datetime,timedelta
from airflow.sdk import dag, task
from src.misc.airflow_utils import alert_via_webhook
from src.misc.helper_functions import convert_economics_mapping_into_df

@dag(
    default_args={
        'owner' : 'mseidl',
        'retries' : 2,
        'email_on_failure': False,
        'retry_delay' : timedelta(minutes=5),
        'on_failure_callback': alert_via_webhook
    },
    dag_id='utility_sql_materialize',
    description='Aggregate materialized views on database',
    tags=['utility', 'daily'],
    start_date=datetime(2023,4,24),
    schedule='30 02 * * *' 
    ## needs to run after Airtable DAG (which included new labels -> relevant for app level metrics) and blockspace DAG
    ## needs to run after other_bq_to_postgres_aads DAG (which includes raw polygon aa data)
)

def etl():
    @task()
    def run_unique_senders():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        adapter_params = {}
        load_params = {
            'load_type' : 'active_addresses_agg',
            'days' : 3, ## days as int or 'auto
        }

       # initialize adapter
        db_connector = DbConnector()
        ad = AdapterSQL(adapter_params, db_connector)
        # extract
        ad.extract(load_params)

    @task()
    def run_da_queries():
        from src.db_connector import DbConnector
        from src.adapters.adapter_sql import AdapterSQL
        adapter_params = {}
        load_params = {
            'load_type' : 'jinja', ## usd_to_eth or metrics or blockspace
            'queries' : ['da_metrics/upsert_fact_da_consumers_celestia_blob_size.sql.j2', 
                         'da_metrics/upsert_fact_da_consumers_celestia_blob_fees.sql.j2', 
                         'da_metrics/upsert_fact_da_consumers_celestia_blob_count.sql.j2',
                         'da_metrics/upsert_fact_kpis_celestia_chain_metrics.sql.j2'
                         ],
        }

        # initialize adapter
        db_connector = DbConnector()
        ad = AdapterSQL(adapter_params, db_connector)

        # extract
        ad.extract(load_params)

    @task()
    def run_economics_mapping():
        import yaml
        from src.db_connector import DbConnector
        from src.gtp_dna import get_gtp_dna_file

        data = yaml.load(get_gtp_dna_file("economics_da/economics_mapping.yml"), Loader=yaml.FullLoader)
        df = convert_economics_mapping_into_df(data)

        ## in column da_layer rename 'celestia' to 'da_celestia', 'L1' to 'da_ethereum_calldata', 'beacon' to 'da_ethereum_blobs'
        df['da_layer'] = df['da_layer'].replace({'celestia': 'da_celestia', 'l1': 'da_ethereum_calldata', 'beacon': 'da_ethereum_blobs'})

        ## replace None with 'NA' in all columns
        df.fillna('NA', inplace=True)

        df.set_index(['origin_key', 'da_layer', 'from_address', 'to_address', 'method', 'namespace'], inplace=True)

        db_connector = DbConnector()
        db_connector.upsert_table('sys_economics_mapping', df)

    @task()
    def run_refresh_materialized_app_view():
        from src.db_connector import DbConnector

        # refresh the materialized view for APP level fact table
        db_connector = DbConnector()
        db_connector.refresh_materialized_view('vw_apps_contract_level_materialized')

    run_unique_senders()
    run_da_queries()
    run_economics_mapping()
    run_refresh_materialized_app_view()
etl()




