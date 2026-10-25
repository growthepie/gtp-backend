import os
import json
import pandas as pd
from datetime import datetime, timezone

from src.misc.helper_functions import upload_json_to_cf_s3, fix_dict_nan, api_get_call
from src.misc.jinja_helper import execute_jinja_query

## ETH asset jsons are only updated by our scheduled jobs, so instead of a CloudFront invalidation per upload we rely on a short CDN cache:
## new data is visible at the latest 5 min (s-maxage) after an upload, browsers cache for 60s, stale copies are served for up to 1 day if S3 errors
ETH_ASSET_CACHE_CONTROL = "public, max-age=60, s-maxage=300, stale-while-revalidate=60, stale-if-error=86400"

## The only files published for the ETH asset page (plus logos), grouped by update frequency. Each section is created in its own DAG task and
## handed to the bundle task via XCom; a section that failed in this run keeps its previously published data (see sections_last_updated_utc).
ETH_ASSET_BUNDLES = {
    'live':  ['kpis', 'stats', 'properties', 'productive_asset'],               ## hourly
    'daily': ['purchasing_power', 'supply_flow', 'today', 'market_caps'],       ## daily (07:35 UTC)
}

## timeframes of the KPI charts on the ETH asset page
## hours: window length based on hourly data, days: window length based on daily data, months: calendar months incl. current (partial) month
ETH_ASSET_TIMEFRAMES = {
    '24h': {'granularity': 'hourly', 'hours': 24, 'bucket_hours': 1},
    '7d':  {'granularity': '3_hours', 'hours': 24*7, 'bucket_hours': 3},
    '60d': {'granularity': 'daily', 'days': 60, 'bucket_days': 1},
    '1y':  {'granularity': 'weekly', 'days': 52*7, 'bucket_days': 7},
    '5y':  {'granularity': 'monthly', 'months': 60},
}

SOURCES = {
    'coingecko': {'name': 'CoinGecko', 'url': 'https://www.coingecko.com/en/coins/ethereum'},
    'ultrasound': {'name': 'ultrasound.money', 'url': 'https://ultrasound.money'},
    'ethsupply': {'name': 'ethsupply.fyi', 'url': 'https://ethsupply.fyi'},
    'growthepie': {'name': 'growthepie', 'url': 'https://www.growthepie.com'},
    'worldbank': {'name': 'World Bank', 'url': 'https://data.worldbank.org/indicator/SP.POP.TOTL'},
    'defillama': {'name': 'DefiLlama', 'url': 'https://defillama.com'},
    'wgc': {'name': 'World Gold Council', 'url': 'https://www.gold.org/goldhub/data/above-ground-stocks'},
    'fred': {'name': 'FRED (Federal Reserve Bank of St. Louis)', 'url': 'https://fred.stlouisfed.org/series/M2SL'},
    'fred_sndr': {'name': 'FRED / FDIC national deposit rates', 'url': 'https://fred.stlouisfed.org/series/SNDR'},
    'fred_dff': {'name': 'FRED / Federal Reserve', 'url': 'https://fred.stlouisfed.org/series/DFF'},
    'fred_ecbdfr': {'name': 'FRED / European Central Bank', 'url': 'https://fred.stlouisfed.org/series/ECBDFR'},
    'bls': {'name': 'US Bureau of Labor Statistics via FRED', 'url': 'https://fred.stlouisfed.org/release/tables?rid=10&eid=34419'},
    'eia_gas': {'name': 'US Energy Information Administration via FRED', 'url': 'https://fred.stlouisfed.org/series/GASREGW'},
    'estat': {'name': 'Statistics Bureau of Japan (e-Stat), Retail Price Survey', 'url': 'https://www.e-stat.go.jp/en/statistics/00200571'},
    'eurostat': {'name': 'Eurostat HICP', 'url': 'https://ec.europa.eu/eurostat/databrowser/view/prc_hicp_minr/default/table'},
    'insee': {'name': 'INSEE average retail prices (France)', 'url': 'https://www.insee.fr/fr/statistiques/serie/000442423'},
    'ons': {'name': 'Office for National Statistics', 'url': 'https://www.ons.gov.uk/economy/inflationandpriceindices'},
    'numbeo': {'name': 'Numbeo (crowd-sourced)', 'url': 'https://www.numbeo.com/cost-of-living/'},
    'fred_fx': {'name': 'FRED exchange rates', 'url': 'https://fred.stlouisfed.org/categories/94'},
    'yahoo': {'name': 'Yahoo Finance', 'url': 'https://finance.yahoo.com'},
    'companiesmarketcap': {'name': 'CompaniesMarketCap', 'url': 'https://companiesmarketcap.com'},
    'blockchain_com': {'name': 'Blockchain.com', 'url': 'https://www.blockchain.com/explorer/charts/total-bitcoins'},
    'eia': {'name': 'EIA via Our World in Data', 'url': 'https://ourworldindata.org/grapher/oil-proved-reserves'},
    'beacon_chain': {'name': 'Ethereum beacon chain (consensus layer node)', 'url': 'https://ethereum.github.io/beacon-APIs/'},
    'aave': {'name': 'Aave v3 Ethereum (onchain)', 'url': 'https://app.aave.com/markets/?marketName=proto_mainnet_v3'},
}

## FRED series are loaded daily into eim_fact by AdapterReferenceData (see FRED_SERIES). M2SL: US dollar money supply (monthly, bn USD),
## SNDR: FDIC national average savings deposit rate (monthly, %), DFF: effective federal funds rate (daily, %),
## ECBDFR: ECB deposit facility rate (daily, %)
FRED_RATE_SERIES = ['M2SL', 'SNDR', 'DFF', 'ECBDFR']
## window for the annualized ETH supply change in the scarcity comparison
ETH_SUPPLY_CHANGE_DAYS = 30
## days of history for the 'ETH paid as gas' timeseries
GAS_PAID_DAYS = 365

## ----- Stake / Lend / Borrow ('ETH is a productive asset' section) -----
## validators & queues from a public beacon node (standard beacon API, ~20MB per run)
BEACON_API_URLS = ['https://ethereum-beacon-api.publicnode.com']
SECONDS_PER_EPOCH = 12 * 32
## Electra churn (EIP-7251): min(256 ETH, max(128 ETH, total active balance / 65536)) per epoch, shared by entries and exits
CHURN_LIMIT_QUOTIENT = 65536
MIN_CHURN_ETH = 128
MAX_ACTIVATION_EXIT_CHURN_ETH = 256
## lending/borrowing: Aave v3 Ethereum core market, the largest ETH lending market (~3x SparkLend by ETH supplied per DefiLlama, Oct 2026)
AAVE_POOL = '0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2'
AAVE_WETH = ('WETH', '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2', 18)
AAVE_STABLES = [('USDC', '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48', 6), ('USDT', '0xdAC17F958D2ee523a2206206994597C13D831ec7', 6)]
AAVE_POOL_ABI = [{'inputs': [{'name': 'asset', 'type': 'address'}], 'name': 'getReserveData', 'stateMutability': 'view', 'type': 'function', 'outputs': [{'type': 'tuple', 'components': [
    {'name': 'configuration', 'type': 'uint256'}, {'name': 'liquidityIndex', 'type': 'uint128'}, {'name': 'currentLiquidityRate', 'type': 'uint128'},
    {'name': 'variableBorrowIndex', 'type': 'uint128'}, {'name': 'currentVariableBorrowRate', 'type': 'uint128'}, {'name': 'currentStableBorrowRate', 'type': 'uint128'},
    {'name': 'lastUpdateTimestamp', 'type': 'uint40'}, {'name': 'id', 'type': 'uint16'}, {'name': 'aTokenAddress', 'type': 'address'},
    {'name': 'stableDebtTokenAddress', 'type': 'address'}, {'name': 'variableDebtTokenAddress', 'type': 'address'}, {'name': 'interestRateStrategyAddress', 'type': 'address'},
    {'name': 'accruedToTreasury', 'type': 'uint128'}, {'name': 'unbacked', 'type': 'uint128'}, {'name': 'isolationModeTotalDebt', 'type': 'uint128'}]}]}]
AAVE_TOTAL_SUPPLY_ABI = [{'inputs': [], 'name': 'totalSupply', 'outputs': [{'type': 'uint256'}], 'stateMutability': 'view', 'type': 'function'}]
AAVE_RATE_STRATEGY_ABI = [{'inputs': [{'name': 'reserve', 'type': 'address'}], 'name': 'getInterestRateDataBps', 'stateMutability': 'view', 'type': 'function', 'outputs': [{'type': 'tuple', 'components': [
    {'name': 'optimalUsageRatio', 'type': 'uint16'}, {'name': 'baseVariableBorrowRate', 'type': 'uint32'}, {'name': 'variableRateSlope1', 'type': 'uint32'}, {'name': 'variableRateSlope2', 'type': 'uint32'}]}]}]

## ----- 'How big is it really' (market caps) -----
## everything is refetched on every run, nothing is hardcoded. Logos are downloaded once and served from our CDN at v1/eth_asset/logos/<key>.png
CMC_BASE_URL = 'https://companiesmarketcap.com'
CMC_COMPANIES_CSV_URL = 'https://companiesmarketcap.com/?download=csv'   ## global company ranking in USD
CMC_ASSETS_URL = 'https://companiesmarketcap.com/assets-by-market-cap/'  ## all assets incl. commodities (gold, silver, platinum)
CMC_LOGO_URL = 'https://companiesmarketcap.com/img/company-logos/128/{}.png'
HTTP_HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36'}
## crypto: CoinGecko top N by market cap (incl. ETH), key = lowercased symbol (logo comes from CoinGecko)
MCAP_CRYPTO_TOP_N = 10
## companies: all with a bigger market cap than ETH + the next 10 below ETH (list length moves with the ETH price)
MCAP_EQUITY_BELOW_ETH = 10
## ETH/BTC market cap ratio, daily history (~5.5 years). Our CoinGecko plan only serves 2 years of market_chart history, so:
##   ETH: fact_kpis.market_cap_usd (CoinGecko, collected daily since 2015)
##   BTC: Yahoo Finance BTC-USD daily close x circulating supply from blockchain.com (matches CoinGecko within ~0.2% on a typical day)
ETH_BTC_RATIO_DAYS = 2000
BTC_SUPPLY_URL = 'https://api.blockchain.info/charts/total-bitcoins?timespan=7years&format=json'
## oil is not listed on companiesmarketcap: world proved reserves (EIA via Our World in Data, latest year read at runtime) x Brent price (Yahoo Finance)
OIL_RESERVES_URL = 'https://ourworldindata.org/grapher/oil-proved-reserves.csv?useColumnShortNames=true'
OIL_LOGO_URL = 'https://financialmodelingprep.com/image-stock/CLUSD.png'
BARRELS_PER_M3 = 1 / 0.158987294928

## ----- 'What one ETH actually buys' (purchasing power) -----
## Prices per region and item. Methods:
##   fred:   absolute US average prices (BLS monthly / EIA weekly) via FRED
##   estat:  absolute retail prices from Japan's Retail Price Survey (Tokyo 23 wards, monthly) via the e-Stat API (env ESTAT_APP_ID)
##   anchor: price = anchor price x index(month) / index(anchor month), index = official monthly CPI/HICP for that item.
##           The anchor price is fetched once from an official series at a fixed month (INSEE, ONS) or, if none exists, set as a fixed value.
## All prices are converted to the same unit in every region (STANDARD_UNITS): factor = standard units per source unit.
LB_IN_G = 453.59237
GALLON_IN_L = 3.785411784
STANDARD_UNITS = {'bread': '500 g loaf', 'eggs': 'dozen', 'coffee': '500 g (ground)', 'fuel': 'litre', 'gold': 'gram'}
PURCHASING_POWER_REGIONS = {
    'us': {'name': 'United States', 'currency': 'USD', 'items': {
        'bread':  {'source_unit': '1 lb (white bread)', 'factor': 500 / LB_IN_G, 'method': 'fred', 'series': 'APU0000702111', 'source': 'bls'},
        'eggs':   {'source_unit': 'dozen (grade A large)', 'factor': 1, 'method': 'fred', 'series': 'APU0000708111', 'source': 'bls'},
        'coffee': {'source_unit': '1 lb (100% ground roast)', 'factor': 500 / LB_IN_G, 'method': 'fred', 'series': 'APU0000717311', 'source': 'bls'},
        'fuel':   {'source_unit': 'gallon (regular gasoline)', 'factor': 1 / GALLON_IN_L, 'method': 'fred', 'series': 'GASREGW', 'source': 'eia_gas'},
    }},
    'euro_area': {'name': 'Euro area', 'currency': 'EUR', 'items': {
        ## anchors from France (INSEE average retail prices), moved with euro area HICP (Eurostat, ECOICOP v2)
        'bread':  {'source_unit': '1 kg (baguette)', 'factor': 0.5, 'method': 'anchor', 'anchor': {'insee': '000442423', 'month': '2025-01'}, 'index': {'eurostat': 'CP01113'}},
        'eggs':   {'source_unit': 'dozen', 'factor': 1, 'method': 'anchor', 'anchor': {'value': 3.65, 'month': '2026-08', 'source': 'numbeo'}, 'index': {'eurostat': 'CP01148'}},
        'coffee': {'source_unit': '250 g (ground)', 'factor': 2, 'method': 'anchor', 'anchor': {'insee': '000442545', 'month': '2019-12'}, 'index': {'eurostat': 'CP01220'}},
        'fuel':   {'source_unit': 'litre (unleaded 95)', 'factor': 1, 'method': 'anchor', 'anchor': {'insee': '000849411', 'month': '2025-01'}, 'index': {'eurostat': 'CP07222'}},
    }},
    'uk': {'name': 'United Kingdom', 'currency': 'GBP', 'items': {
        ## anchors: last ONS average prices (RPI average price series, discontinued, in pence), moved with ONS CPI item indices
        'bread':  {'source_unit': '800 g (white sliced)', 'factor': 500 / 800, 'method': 'anchor', 'anchor': {'ons': 'czoh', 'month': '2025-01', 'scale': 0.01}, 'index': {'ons': 'l78j'}},
        'eggs':   {'source_unit': 'dozen', 'factor': 1, 'method': 'anchor', 'anchor': {'ons': 'j428', 'month': '2025-01', 'scale': 0.01}, 'index': {'ons': 'l79a'}},
        'coffee': {'source_unit': '227 g (ground)', 'factor': 500 / 227, 'method': 'anchor', 'anchor': {'ons': 'czno', 'month': '2021-01', 'scale': 0.01}, 'index': {'ons': 'l7a6'}},
        'fuel':   {'source_unit': 'litre (unleaded petrol)', 'factor': 1, 'method': 'anchor', 'anchor': {'ons': 'czmk', 'month': '2025-01', 'scale': 0.01}, 'index': {'ons': 'l7fp'}},
    }},
    'japan': {'name': 'Japan', 'currency': 'JPY', 'items': {
        'bread':  {'source_unit': '1 kg (食パン)', 'factor': 0.5, 'method': 'estat', 'series': '01021'},
        'eggs':   {'source_unit': 'pack of 10 (鶏卵)', 'factor': 1.2, 'method': 'estat', 'series': '01341'},
        'coffee': {'source_unit': '100 g (ground, コーヒー豆)', 'factor': 5, 'method': 'estat', 'series': '01922'},
        'fuel':   {'source_unit': 'litre (regular gasoline)', 'factor': 1, 'method': 'estat', 'series': '07301'},
    }},
}
PURCHASING_POWER_ITEM_NAMES = {'bread': 'Bread', 'eggs': 'Eggs', 'coffee': 'Coffee', 'fuel': 'Fuel', 'gold': 'Gold'}
## FX from FRED (daily): USD per unit of local currency (JPY is quoted the other way round and inverted)
FX_SERIES = {'EUR': ('DEXUSEU', False), 'GBP': ('DEXUSUK', False), 'JPY': ('DEXJPUS', True)}
## all FRED series used on the ETH asset page
FRED_SERIES = FRED_RATE_SERIES + [s for s, _ in FX_SERIES.values()] + \
    [c['series'] for r in PURCHASING_POWER_REGIONS.values() for c in r['items'].values() if c['method'] == 'fred']
ESTAT_RETAIL_PRICE_TABLE = '0003421913'   ## Retail Price Survey, major items by city (monthly)
ESTAT_TOKYO_AREA = '13100'                ## Tokyo 23 wards
EUROSTAT_HICP_URL = 'https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/prc_hicp_minr?format=JSON&geo=EA&unit=I25&coicop18={}'
INSEE_SERIES_URL = 'https://www.bdm.insee.fr/series/sdmx/data/SERIES_BDM/{}'
ONS_SERIES_URL = 'https://www.ons.gov.uk/generator?format=csv&uri=/economy/inflationandpriceindices/timeseries/{}/mm23'
TROY_OZ_IN_GRAMS = 31.1034768

## ----- 'The supply, in one picture' (issuance tap vs. burn drain) -----
## scenarios = real historical 7-day windows:
##   quiet = 10th percentile of weekly burn over the last year, busy = 90th percentile of weekly burn since the Merge
##   (same PoS issuance regime as today), mania = highest weekly burn since London
SUPPLY_FLOW_QUIET_QUANTILE = 0.1
SUPPLY_FLOW_QUIET_LOOKBACK_DAYS = 365
SUPPLY_FLOW_BUSY_QUANTILE = 0.9
SUPPLY_FLOW_BUSY_SINCE = '2022-09-15'  ## the Merge

## World Bank Population Estimates and Projections (source 40), mid-year (July 1) world population incl. projections
WORLD_POPULATION_URL = 'https://api.worldbank.org/v2/country/WLD/indicator/SP.POP.TOTL?format=json&source=40&date=2020:2040&per_page=100'
## window to derive per_second_increase of total wallets (7d is noticeably noisier due to HLL estimation error)
TOTAL_WALLETS_GROWTH_DAYS = 30

## col: column in the prepared hourly/daily dataframes
## agg: how values are aggregated into a bucket and into the 'current' value
##   last -> stock (value at end of bucket, current = latest value, change vs. value one window ago)
##   mean -> rate (avg over bucket, current = latest value, change vs. value one window ago)
##   sum  -> flow (total in bucket, current = total in window, change vs. total in previous window)
ETH_ASSET_KPIS = {
    'eth_price':     {'name': 'ETH Price', 'unit': 'usd', 'col': 'price_usd', 'agg': 'last', 'sources': ['coingecko']},
    'eth_supply':    {'name': 'ETH Supply', 'unit': 'eth', 'col': 'eth_supply_eth', 'agg': 'last', 'sources': ['ultrasound']},
    'staking_yield': {'name': 'Staking Yield', 'unit': 'percent', 'col': 'staking_yield', 'agg': 'mean', 'sources': ['ethsupply', 'growthepie']},
    'staked_share':  {'name': 'Share of Supply Staked', 'unit': 'percent', 'col': 'staked_share', 'agg': 'last', 'sources': ['ethsupply', 'growthepie', 'ultrasound']},
    'eth_burned':    {'name': 'ETH Burned', 'unit': 'eth', 'col': 'eth_burned_eth', 'agg': 'sum', 'sources': ['growthepie']},
}

STOCK_COLS = ['price_usd', 'eth_supply_eth', 'eth_staked_eth']


## section dicts are only published inside the live/daily bundles; locally (no S3 bucket) each section is also written to output/ for inspection
def write_local_section(s3_bucket, api_version:str, section:str, details_dict:dict):
    if s3_bucket is not None:
        return
    os.makedirs(f'output/{api_version}/eth_asset', exist_ok=True)
    with open(f'output/{api_version}/eth_asset/{section}.json', 'w') as fp:
        json.dump(details_dict, fp)

class EthAssetJSONCreation():
    def __init__(self, s3_bucket, cf_distribution_id, db_connector, api_version='v1'):
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector
        self.api_version = api_version

    ## ----------------- Data loading --------------------

    def get_hourly_data(self, days:int=16):
        exec_string = f"""
            SELECT metric_key, "timestamp", value
            FROM public.fact_kpis_granular
            WHERE origin_key = 'ethereum'
                AND granularity = 'hourly'
                AND metric_key IN ('price_usd', 'eth_supply_eth', 'eth_burned_eth', 'eth_staked_eth', 'staking_rewards_cl_eth')
                AND "timestamp" >= date_trunc('hour', now() at time zone 'utc') - interval '{days} days'
        """
        df = self.db_connector.execute_query(exec_string, load_df=True)
        return df.pivot_table(index='timestamp', columns='metric_key', values='value', aggfunc='last').sort_index()

    def get_daily_data(self, days:int=6*366):
        exec_string = f"""
            SELECT metric_key, "date", value
            FROM public.fact_kpis
            WHERE origin_key = 'ethereum' AND metric_key = 'price_usd'
                AND "date" >= current_date - interval '{days} days'
            UNION ALL
            SELECT metric_key, "date", value
            FROM public.eim_fact
            WHERE origin_key = 'ethereum'
                AND metric_key IN ('eth_supply_eth', 'eth_burned_eth', 'eth_staked_eth', 'staking_yield_cl', 'staking_yield_el', 'staking_yield_tips', 'staking_yield_mev')
                AND "date" >= current_date - interval '{days} days'
        """
        df = self.db_connector.execute_query(exec_string, load_df=True)
        df['date'] = pd.to_datetime(df['date'])
        df = df.pivot_table(index='date', columns='metric_key', values='value', aggfunc='last').sort_index()
        ## only complete days
        today = pd.Timestamp(datetime.now(timezone.utc).date())
        return df[df.index < today]

    ## ----------------- Preparation --------------------

    ## complete index until end, forward fill stocks (e.g. staked ETH can lag a few hours)
    def complete_index(self, df:pd.DataFrame, end:pd.Timestamp, freq:str):
        df.index = df.index.astype('datetime64[ns]')
        idx = pd.date_range(start=df.index.min(), end=end, freq=freq).astype('datetime64[ns]')
        df = df.reindex(df.index.union(idx))
        for col in STOCK_COLS:
            if col in df.columns:
                df[col] = df[col].ffill()
        return df.reindex(idx)

    ## staking yield (CL + EL) as 7d rolling average of daily values
    def prepare_daily(self, df:pd.DataFrame, end_day:pd.Timestamp):
        df = self.complete_index(df, end_day, 'D')
        df['staking_yield'] = (df['staking_yield_cl'].ffill() + df['staking_yield_el'].ffill()).rolling(7, min_periods=1).mean()
        df['staked_share'] = df['eth_staked_eth'] / df['eth_supply_eth']
        return df

    ## staking yield = hourly CL yield (trailing 24h avg) + EL yield (7d avg of daily, EL is only available daily)
    ## if hourly CL rewards are missing (ethsupply.fyi fallback), the daily 7d avg CL yield is used
    def prepare_hourly(self, df:pd.DataFrame, df_daily:pd.DataFrame, end_hour:pd.Timestamp):
        df = self.complete_index(df, end_hour, 'h')

        df_d = pd.DataFrame({
            'cl_7d': df_daily['staking_yield_cl'].rolling(7, min_periods=1).mean(),
            'el_7d': df_daily['staking_yield_el'].rolling(7, min_periods=1).mean(),
            'tips_7d': df_daily['staking_yield_tips'].rolling(7, min_periods=1).mean(),
            'mev_7d': df_daily['staking_yield_mev'].rolling(7, min_periods=1).mean(),
        }).dropna(how='all')
        df_d = pd.merge_asof(pd.DataFrame(index=df.index), df_d, left_index=True, right_index=True, direction='backward')

        cl_hourly = df['staking_rewards_cl_eth'] * 24 * 365 / df['eth_staked_eth']
        cl = cl_hourly.rolling(24, min_periods=1).mean().fillna(df_d['cl_7d'])
        df['staking_yield'] = cl + df_d['el_7d'].ffill()
        df['staked_share'] = df['eth_staked_eth'] / df['eth_supply_eth']
        ## components, used for the yield breakdown (tips + mev = el)
        df['staking_yield_cl_24h'] = cl
        df['staking_yield_tips_7d'] = df_d['tips_7d'].ffill()
        df['staking_yield_mev_7d'] = df_d['mev_7d'].ffill()
        return df

    ## ----------------- Bucketing --------------------

    ## assigns every row in [start, end] a bucket label (bucket start), counted from window start so all buckets are complete
    def bucketize(self, df:pd.DataFrame, start:pd.Timestamp, end:pd.Timestamp, tf:dict):
        df = df[(df.index >= start) & (df.index <= end)].copy()
        if 'bucket_hours' in tf:
            step = pd.Timedelta(hours=tf['bucket_hours'])
        elif 'bucket_days' in tf:
            step = pd.Timedelta(days=tf['bucket_days'])
        else:
            df['bucket'] = df.index.to_period('M').to_timestamp()
            return df
        df['bucket'] = start + ((df.index - start) // step) * step
        return df

    def window_bounds(self, tf:dict, end_hour:pd.Timestamp, end_day:pd.Timestamp, shift:int=0):
        if 'hours' in tf:
            end = end_hour - pd.Timedelta(hours=tf['hours'] * shift)
            return end - pd.Timedelta(hours=tf['hours'] - 1), end
        if 'days' in tf:
            end = end_day - pd.Timedelta(days=tf['days'] * shift)
            return end - pd.Timedelta(days=tf['days'] - 1), end
        end = end_day
        start = (end.to_period('M') - (tf['months'] - 1)).to_timestamp()
        return start, end

    def aggregate_buckets(self, df:pd.DataFrame, col:str, agg:str):
        g = df.groupby('bucket')[col]
        if agg == 'last':
            return g.last()
        if agg == 'sum':
            return g.sum(min_count=1)
        return g.mean()

    def value_at(self, s:pd.Series, ts:pd.Timestamp):
        s = s[s.index <= ts].dropna()
        return s.iloc[-1] if len(s) > 0 else None

    def pct_change(self, current, previous):
        if current is None or previous is None or pd.isna(current) or pd.isna(previous) or previous == 0:
            return None
        return round((current - previous) / previous, 6)

    ## ----------------- JSON --------------------

    def create_kpi_dict(self, df_hourly_raw:pd.DataFrame, df_daily_raw:pd.DataFrame):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        end_hour = pd.Timestamp(now).floor('h') - pd.Timedelta(hours=1) ## last complete hour
        end_day = pd.Timestamp(now.date()) - pd.Timedelta(days=1) ## last complete day

        df_daily = self.prepare_daily(df_daily_raw, end_day)
        df_hourly = self.prepare_hourly(df_hourly_raw, df_daily, end_hour)

        kpi_dict = {}
        for kpi, cfg in ETH_ASSET_KPIS.items():
            col, agg = cfg['col'], cfg['agg']
            kpi_dict[kpi] = {
                'name': cfg['name'],
                'unit': cfg['unit'],
                'sources': [SOURCES[s] for s in cfg['sources']],
                'timeframes': {},
            }

            for tf_key, tf in ETH_ASSET_TIMEFRAMES.items():
                hourly_tf = 'hours' in tf
                df_src = df_hourly if hourly_tf else df_daily
                start, end = self.window_bounds(tf, end_hour, end_day)

                df_tf = self.bucketize(df_src, start, end, tf)
                s_buckets = self.aggregate_buckets(df_tf, col, agg)

                if agg == 'sum':
                    ## flow: total in window vs. total in previous window (data has to cover the full previous window)
                    current = df_tf[col].sum(min_count=1)
                    change = None
                    if 'months' not in tf:
                        prev_start, prev_end = self.window_bounds(tf, end_hour, end_day, shift=1)
                        df_prev = df_src[(df_src.index >= prev_start) & (df_src.index <= prev_end)]
                        first_valid = df_prev[col].first_valid_index()
                        if first_valid is not None and first_valid <= prev_start:
                            change = self.pct_change(current, df_prev[col].sum(min_count=1))
                else:
                    ## stock/rate: latest hourly value (same for all timeframes) vs. value one window ago
                    current = self.value_at(df_hourly[col], end_hour)
                    change = None
                    if hourly_tf:
                        change = self.pct_change(current, self.value_at(df_hourly[col], end_hour - pd.Timedelta(hours=tf['hours'])))
                    elif 'days' in tf:
                        ## daily values are start of day snapshots, window start = 'days' ago
                        prev = df_daily[col].get(start) if df_daily.index.min() <= start else None
                        change = self.pct_change(current, prev)

                ## unix ms, independent of the datetime resolution (ns/us/s) of the index
                unix = (s_buckets.index - pd.Timestamp('1970-01-01')) // pd.Timedelta(milliseconds=1)
                data = [[int(u), None if pd.isna(v) else float(v)] for u, v in zip(unix, s_buckets.values)]

                kpi_dict[kpi]['timeframes'][tf_key] = {
                    'granularity': tf['granularity'],
                    'types': ['unix', 'value'],
                    'data': data,
                    'current': None if current is None or pd.isna(current) else float(current),
                    'change_pct': change,
                }
        return kpi_dict

    ## ----------------- Stats (single values, extrapolated per second by the frontend) --------------------

    ## lifetime unique addresses that initiated a tx on Ethereum (same as the 'daa' lifetime achievement on the chain page)
    ## value as of today 00:00 UTC (only complete days), per_second_increase = avg growth over the last 30 days
    def get_total_wallets(self):
        total = execute_jinja_query(self.db_connector, 'api/select_total_aa.sql.j2', {'origin_key': 'ethereum'}, return_df=True).iloc[0]['value']
        total_prev = execute_jinja_query(self.db_connector, 'api/select_total_aa.sql.j2', {'origin_key': 'ethereum', 'days': TOTAL_WALLETS_GROWTH_DAYS}, return_df=True).iloc[0]['value']
        ts = pd.Timestamp(datetime.now(timezone.utc).date(), tz='UTC')
        return {
            'name': 'Total Wallets',
            'unit': 'addresses',
            'value': int(total),
            'timestamp': int(ts.timestamp() * 1000),
            'per_second_increase': float(total - total_prev) / (TOTAL_WALLETS_GROWTH_DAYS * 86400),
            'sources': [SOURCES['growthepie']],
        }

    ## world population at ts, linearly interpolated between mid-year values, plus growth per second of the current segment
    def get_world_population(self, ts:pd.Timestamp):
        response_json = api_get_call(WORLD_POPULATION_URL, sleeper=10, retries=5)
        if not response_json or len(response_json) < 2 or not response_json[1]:
            raise ValueError("No world population data returned from World Bank API")
        pop = pd.Series({pd.Timestamp(f"{r['date']}-07-01", tz='UTC'): float(r['value']) for r in response_json[1] if r['value'] is not None}).sort_index()

        before, after = pop[pop.index <= ts], pop[pop.index > ts]
        if before.empty or after.empty:
            raise ValueError(f"World population data does not cover {ts}")
        t0, p0, t1, p1 = before.index[-1], before.iloc[-1], after.index[0], after.iloc[0]
        per_second = (p1 - p0) / (t1 - t0).total_seconds()
        return p0 + per_second * (ts - t0).total_seconds(), per_second

    ## ETH supply split evenly across world population
    ## per_second_increase = d(S/P)/dt = S'/P - S*P'/P^2, with S' = avg supply change per second over the last 7 days (hourly supply)
    def get_eth_per_person(self):
        df = self.db_connector.execute_query("""
            SELECT "timestamp", value
            FROM public.fact_kpis_granular
            WHERE origin_key = 'ethereum' AND granularity = 'hourly' AND metric_key = 'eth_supply_eth'
                AND "timestamp" >= date_trunc('hour', now() at time zone 'utc') - interval '7 days'
            ORDER BY 1
        """, load_df=True)
        if df.shape[0] < 2:
            raise ValueError("Not enough hourly supply data for eth_per_person")
        t0, s0 = pd.Timestamp(df['timestamp'].iloc[0], tz='UTC'), df['value'].iloc[0]
        ts, supply = pd.Timestamp(df['timestamp'].iloc[-1], tz='UTC'), df['value'].iloc[-1]
        supply_per_second = (supply - s0) / (ts - t0).total_seconds()

        population, population_per_second = self.get_world_population(ts)
        return {
            'name': 'ETH per Person',
            'unit': 'eth',
            'value': supply / population,
            'timestamp': int(ts.timestamp() * 1000),
            'per_second_increase': supply_per_second / population - supply * population_per_second / population**2,
            'sources': [SOURCES['ultrasound'], SOURCES['worldbank']],
            'details': {
                'eth_supply': supply,
                'eth_supply_per_second': supply_per_second,
                'world_population': population,
                'world_population_per_second': population_per_second,
            },
        }

    def create_stats_json(self):
        details_dict = {
            'data': {
                'total_wallets': self.get_total_wallets(),
                'eth_per_person': self.get_eth_per_person(),
            },
            'last_updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        }
        details_dict = fix_dict_nan(details_dict, 'eth_asset/stats')

        write_local_section(self.s3_bucket, self.api_version, 'stats', details_dict)
        print('DONE -- ETH asset stats json export done')
        return details_dict

    ## ----------------- Properties ('ETH is 6 things at once' section) --------------------

    ## current staking yield split into issuance (CL), tips and MEV; same total as staking_yield 'current' in kpis.json
    def get_yield_breakdown(self, df_hourly:pd.DataFrame, end_hour:pd.Timestamp):
        row = df_hourly.loc[:end_hour].ffill().iloc[-1]
        issuance, tips, mev = float(row['staking_yield_cl_24h']), float(row['staking_yield_tips_7d']), float(row['staking_yield_mev_7d'])
        return {
            'name': 'Staking Yield Breakdown',
            'unit': 'percent',
            'total': issuance + tips + mev,
            'breakdown': {'issuance': issuance, 'tips': tips, 'mev': mev},
            'sources': [SOURCES['ethsupply'], SOURCES['growthepie']],
        }

    ## annual change of total stock: ETH (last 30 days annualized), gold (WGC year-end stocks), US dollar (M2 YoY)
    def get_scarcity(self, df_hourly:pd.DataFrame, df_daily:pd.DataFrame, end_hour:pd.Timestamp):
        s_now = self.value_at(df_hourly['eth_supply_eth'], end_hour)
        then = pd.Timestamp(end_hour.date()) - pd.Timedelta(days=ETH_SUPPLY_CHANGE_DAYS)
        s_then = df_daily['eth_supply_eth'].get(then)
        eth_change = (s_now / s_then - 1) * (365 * 86400) / (end_hour - then).total_seconds()

        ## gold: latest WGC above-ground stock vs. the latest value at least a year older (else the oldest), annualized
        ## stocks are scraped daily from gold.org (AdapterReferenceData) and stored per period end in eim_fact
        gold = self.db_connector.execute_query("""
            SELECT "date", value FROM public.eim_fact WHERE origin_key = 'gold' AND metric_key = 'gold_stock_tonnes' ORDER BY 1
        """, load_df=True)
        gold['date'] = pd.to_datetime(gold['date'])
        g_latest = gold.iloc[-1]
        g_prev = gold[gold['date'] <= g_latest['date'] - pd.Timedelta(days=365)]
        g_prev = g_prev.iloc[-1] if len(g_prev) > 0 else gold.iloc[0]
        gold_days = (g_latest['date'] - g_prev['date']).days
        gold_change = (g_latest['value'] / g_prev['value'] - 1) * 365 / gold_days

        m2 = self.get_fred_series('M2SL')
        m2_latest = m2.index.max()
        usd_change = m2.iloc[-1] / m2.get(m2_latest - pd.DateOffset(years=1)) - 1

        return {
            'name': 'Annual Change in Total Stock',
            'unit': 'percent',
            'assets': {
                'eth': {'name': 'ETH', 'value': float(eth_change), 'as_of': end_hour.strftime('%Y-%m-%d %H:%M'),
                        'method': f'ETH supply change over the last {ETH_SUPPLY_CHANGE_DAYS} days, annualized', 'sources': [SOURCES['ultrasound']]},
                'gold': {'name': 'Gold', 'value': float(gold_change), 'as_of': g_latest['date'].strftime('%Y-%m-%d'),
                         'method': f"Above-ground gold stock, {g_latest['date']:%Y-%m-%d} vs. {g_prev['date']:%Y-%m-%d}, annualized", 'sources': [SOURCES['wgc']]},
                'usd': {'name': 'US dollar', 'value': float(usd_change), 'as_of': m2_latest.strftime('%Y-%m'),
                        'method': 'M2 money supply, year over year', 'sources': [SOURCES['fred']]},
            },
        }

    ## FRED series as float Series indexed by date, loaded daily into eim_fact by AdapterReferenceData (load_type 'fred')
    def get_fred_series(self, series_id:str):
        df = self.db_connector.execute_query(f"""
            SELECT "date", value FROM public.eim_fact WHERE origin_key = 'fred' AND metric_key = '{series_id.lower()}' ORDER BY 1
        """, load_df=True)
        if df.empty:
            raise ValueError(f"No FRED {series_id} data in eim_fact, run AdapterReferenceData with load_type 'fred'")
        return pd.Series(df['value'].astype(float).values, index=pd.to_datetime(df['date']))

    ## yield an asset pays just for holding it (nominal, annual), as fractions
    def get_yield_comparison(self, staking_yield:float):
        import yfinance as yf
        sndr = self.get_fred_series('SNDR')
        dff = self.get_fred_series('DFF')
        ecb = self.get_fred_series('ECBDFR')

        ## S&P 500 dividend yield: SPY dividends of the last 12 months / current price
        spy = yf.Ticker('SPY')
        div = spy.dividends
        div.index = div.index.tz_localize(None)
        div_12m = div[div.index > pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None)) - pd.Timedelta(days=365)].sum()
        spy_yield = float(div_12m) / float(spy.fast_info.get('lastPrice'))

        assets = [
            {'key': 'eth_staked', 'name': 'ETH staked', 'value': staking_yield, 'as_of': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
             'method': 'Staking yield (issuance + tips + MEV), same as staking_yield in kpis.json', 'sources': [SOURCES['ethsupply'], SOURCES['growthepie']]},
            {'key': 'fed_funds_rate', 'name': 'US central bank rate', 'value': float(dff.iloc[-1]) / 100, 'as_of': dff.index[-1].strftime('%Y-%m-%d'),
             'method': 'Effective federal funds rate (Federal Reserve)', 'sources': [SOURCES['fred_dff']]},
            {'key': 'ecb_rate', 'name': 'ECB rate', 'value': float(ecb.iloc[-1]) / 100, 'as_of': ecb.index[-1].strftime('%Y-%m-%d'),
             'method': 'ECB deposit facility rate', 'sources': [SOURCES['fred_ecbdfr']]},
            {'key': 'sp500_dividend', 'name': 'S&P 500 dividend', 'value': spy_yield, 'as_of': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
             'method': 'Dividends of the last 12 months / price of SPY (S&P 500 ETF)', 'sources': [SOURCES['yahoo']]},
            {'key': 'gold', 'name': 'Gold', 'value': 0.0, 'as_of': None,
             'method': 'Gold pays no yield for holding it', 'sources': []},
            {'key': 'bitcoin', 'name': 'Bitcoin', 'value': 0.0, 'as_of': None,
             'method': 'Bitcoin pays no yield for holding it (no native staking)', 'sources': []},
            {'key': 'cash_savings', 'name': 'Cash savings', 'value': float(sndr.iloc[-1]) / 100, 'as_of': sndr.index[-1].strftime('%Y-%m'),
             'method': 'FDIC national average rate on US savings deposits', 'sources': [SOURCES['fred_sndr']]},
        ]
        return {'name': 'Yield on the Asset Itself', 'unit': 'percent', 'assets': assets}

    def get_beacon_data(self, path:str):
        import requests
        last_error = None
        for url in BEACON_API_URLS:
            try:
                response = requests.get(f'{url}{path}', timeout=180)
                response.raise_for_status()
                return response.json()['data']
            except Exception as e:
                print(f"...beacon API {url}{path} failed: {e}")
                last_error = e
        raise last_error

    ## active validators and entry/exit queue wait from the beacon state at head
    def get_validator_queues(self, staked_eth:float):
        slot = int(self.get_beacon_data('/eth/v1/beacon/headers/head')['header']['message']['slot'])
        epoch = slot // 32
        ## every active validator sits in exactly one committee per epoch
        active_validators = sum(len(c['validators']) for c in self.get_beacon_data('/eth/v1/beacon/states/head/committees'))
        ## exit queue: exit epochs are assigned when an exit is requested, the latest one assigned is where a new exit would start
        exiting = self.get_beacon_data('/eth/v1/beacon/states/head/validators?status=active_exiting')
        exiting_eth = sum(int(v['validator']['effective_balance']) for v in exiting) / 1e9
        last_exit_epoch = max([int(v['validator']['exit_epoch']) for v in exiting], default=epoch)
        exit_days = max(last_exit_epoch - epoch, 0) * SECONDS_PER_EPOCH / 86400
        ## entry queue: pending deposits are processed with the same churn limit as exits
        entering_eth = sum(int(d['amount']) for d in self.get_beacon_data('/eth/v1/beacon/states/head/pending_deposits')) / 1e9
        churn_eth_per_day = min(MAX_ACTIVATION_EXIT_CHURN_ETH, max(MIN_CHURN_ETH, staked_eth / CHURN_LIMIT_QUOTIENT)) * 86400 / SECONDS_PER_EPOCH
        return {
            'active_validators': active_validators,
            'entry_queue': {'eth': entering_eth, 'days': entering_eth / churn_eth_per_day},
            'exit_queue': {'eth': exiting_eth, 'validators': len(exiting), 'days': exit_days},
            'churn_limit_eth_per_day': churn_eth_per_day,
            'slot': slot,
        }

    ## Aave v3 reserve state, rates are APRs (not compounded) as fractions
    def get_aave_reserve(self, w3, asset:tuple):
        name, address, decimals = asset
        pool = w3.eth.contract(address=w3.to_checksum_address(AAVE_POOL), abi=AAVE_POOL_ABI)
        r = pool.functions.getReserveData(w3.to_checksum_address(address)).call()
        cfg = r[0]
        supplied = w3.eth.contract(address=r[8], abi=AAVE_TOTAL_SUPPLY_ABI).functions.totalSupply().call() / 10**decimals
        borrowed = w3.eth.contract(address=r[10], abi=AAVE_TOTAL_SUPPLY_ABI).functions.totalSupply().call() / 10**decimals
        u_opt, base, slope1, slope2 = [x / 10000 for x in w3.eth.contract(address=r[11], abi=AAVE_RATE_STRATEGY_ABI).functions.getInterestRateDataBps(w3.to_checksum_address(address)).call()]
        borrow_apr = r[4] / 1e27
        utilisation = borrowed / supplied if supplied else 0.0

        ## borrow rate = base + slope1 * U/U_opt below the kink, + slope2 * (U - U_opt)/(1 - U_opt) above it
        ## components are scaled to the onchain rate (Aave's utilisation also counts unbacked/virtual balances, tiny differences)
        utilisation_part = slope1 * min(utilisation / u_opt, 1)
        above_kink_part = slope2 * max(utilisation - u_opt, 0) / (1 - u_opt)
        model = base + utilisation_part + above_kink_part
        scale = borrow_apr / model if model > 0 else 0
        return {
            'asset': name,
            'supply_apr': r[2] / 1e27,
            'borrow_apr': borrow_apr,
            'borrow_apr_breakdown': {'base_rate': base * scale, 'utilisation': utilisation_part * scale, 'above_kink_premium': above_kink_part * scale},
            'utilisation': utilisation,
            'optimal_utilisation': u_opt,
            'reserve_factor': ((cfg >> 64) & 0xFFFF) / 10000,
            'max_ltv': (cfg & 0xFFFF) / 10000,
            'liquidation_threshold': ((cfg >> 16) & 0xFFFF) / 10000,
            'supplied': supplied,
            'borrowed': borrowed,
            'available_liquidity': supplied - borrowed,
        }

    def get_aave_reserves(self):
        from web3 import Web3
        rpcs = [self.db_connector.get_special_use_rpc('ethereum')] + self.db_connector.get_realtime_rpcs_for_chain('ethereum')
        last_error = None
        for rpc in [r for r in rpcs if r]:
            try:
                w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={'timeout': 30}))
                return {a[0]: self.get_aave_reserve(w3, a) for a in [AAVE_WETH] + AAVE_STABLES}
            except Exception as e:
                print(f"...Aave read failed on {rpc[:40]}: {e}")
                last_error = e
        raise last_error

    ## three things you can do with ETH: stake it, lend it, borrow against it
    def get_productive_uses(self, df_hourly:pd.DataFrame, end_hour:pd.Timestamp, price_usd:float):
        from src.misc.helper_functions import send_discord_message
        yield_breakdown = self.get_yield_breakdown(df_hourly, end_hour)
        staked = self.value_at(df_hourly['eth_staked_eth'], end_hour)
        supply = self.value_at(df_hourly['eth_supply_eth'], end_hour)
        try:
            queues = self.get_validator_queues(staked)
        except Exception as e:
            print(f"Validator queue data failed: {e}")
            send_discord_message(f"api_eth_asset: beacon API failed, productive_uses.stake validator/queue fields are null: {e}")
            queues = {'active_validators': None, 'entry_queue': None, 'exit_queue': None, 'churn_limit_eth_per_day': None}

        reserves = self.get_aave_reserves()
        weth = reserves[AAVE_WETH[0]]
        stables = [reserves[s[0]] for s in AAVE_STABLES]
        ## stablecoin borrow rate: weighted by amount borrowed (USDC, USDT ~ $1)
        total_borrowed = sum(s['borrowed'] for s in stables)
        weighted = lambda f: sum(f(s) * s['borrowed'] for s in stables) / total_borrowed
        lending_eth = self.get_collateral(price_usd)['breakdown'].get('lending', {}).get('eth')
        market = {'name': 'Aave v3', 'chain': 'Ethereum', 'url': SOURCES['aave']['url']}

        return {
            'price_usd': price_usd,
            'stake': {
                'apr': yield_breakdown['total'],
                'apr_breakdown': yield_breakdown['breakdown'],
                'paid_in': 'ETH',
                'total_staked_eth': staked,
                'share_of_supply': staked / supply,
                **queues,
                'sources': [SOURCES['ethsupply'], SOURCES['growthepie'], SOURCES['beacon_chain']],
            },
            'lend': {
                'apr': weth['supply_apr'],
                'apr_breakdown': {'borrower_interest': weth['supply_apr']},
                'borrow_apr': weth['borrow_apr'],
                'utilisation': weth['utilisation'],
                'reserve_factor': weth['reserve_factor'],
                'market': market,
                'market_supplied_eth': weth['supplied'],
                'market_available_liquidity_eth': weth['available_liquidity'],
                'lending_supplied_eth': lending_eth,
                'sources': [SOURCES['aave'], SOURCES['defillama'], SOURCES['growthepie']],
            },
            'borrow': {
                'apr': weighted(lambda s: s['borrow_apr']),
                'apr_breakdown': {k: weighted(lambda s: s['borrow_apr_breakdown'][k]) for k in ['base_rate', 'utilisation', 'above_kink_premium']},
                'stablecoins': {s['asset']: {k: s[k] for k in ['borrow_apr', 'borrow_apr_breakdown', 'utilisation', 'optimal_utilisation', 'borrowed']} for s in stables},
                'max_ltv': weth['max_ltv'],
                'liquidation_threshold': weth['liquidation_threshold'],
                'market': market,
                'eth_collateral_eth': lending_eth,
                'eth_collateral_usd': lending_eth * price_usd if lending_eth else None,
                'sources': [SOURCES['aave'], SOURCES['defillama'], SOURCES['growthepie']],
            },
        }

    ## 'ETH as a productive asset' section: yield comparison with other assets + stake / lend / borrow
    def create_productive_asset_json(self):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        end_hour = pd.Timestamp(now).floor('h') - pd.Timedelta(hours=1)
        end_day = pd.Timestamp(now.date()) - pd.Timedelta(days=1)
        df_daily = self.prepare_daily(self.get_daily_data(), end_day)
        df_hourly = self.prepare_hourly(self.get_hourly_data(), df_daily, end_hour)
        staking_yield = self.get_yield_breakdown(df_hourly, end_hour)['total']

        details_dict = {
            'data': {
                'yield_comparison': self.get_yield_comparison(staking_yield),
                'productive_uses': self.get_productive_uses(df_hourly, end_hour, self.value_at(df_hourly['price_usd'], end_hour)),
            },
            'last_updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        }
        details_dict = fix_dict_nan(details_dict, 'eth_asset/productive_asset')

        write_local_section(self.s3_bucket, self.api_version, 'productive_asset', details_dict)
        print('DONE -- ETH asset productive asset json export done')
        return details_dict

    ## ----- monthly price/index series (all indexed by month start) -----

    def get_eurostat_hicp(self, coicop:str):
        response_json = api_get_call(EUROSTAT_HICP_URL.format(coicop), sleeper=10, retries=5)
        if not response_json or not response_json.get('value'):
            raise ValueError(f"No HICP data returned from Eurostat for {coicop}")
        months = {pos: m for m, pos in response_json['dimension']['time']['category']['index'].items()}
        return pd.Series({pd.Timestamp(months[int(pos)]): float(v) for pos, v in response_json['value'].items()}).sort_index()

    def get_insee_series(self, idbank:str):
        import re, requests
        r = requests.get(INSEE_SERIES_URL.format(idbank), headers=HTTP_HEADERS, timeout=60)
        r.raise_for_status()
        obs = re.findall(r'TIME_PERIOD="(\d{4}-\d{2})" OBS_VALUE="([\d.]+)"', r.text)
        if not obs:
            raise ValueError(f"No data returned from INSEE for {idbank}")
        return pd.Series({pd.Timestamp(t): float(v) for t, v in obs}).sort_index()

    ## ONS rate limits quick successive requests (HTTP 429): pause between calls and back off on 429
    def get_ons_series(self, cdid:str):
        import io, time, requests
        for attempt in range(6):
            time.sleep(2 + attempt * 15)
            r = requests.get(ONS_SERIES_URL.format(cdid), headers=HTTP_HEADERS, timeout=60)
            if r.status_code != 429:
                break
            print(f"...ONS rate limit for {cdid}, retrying")
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), header=None, names=['period', 'value'])
        df = df[df['period'].astype(str).str.match(r'^\d{4} [A-Z]{3}$')]
        return pd.Series(df['value'].astype(float).values, index=pd.to_datetime(df['period'], format='%Y %b')).sort_index()

    ## Japan Retail Price Survey, Tokyo 23 wards: {item code: monthly price series in JPY}
    def get_estat_prices(self, item_codes:list):
        import os
        start = (pd.Timestamp(datetime.now(timezone.utc).date()) - pd.DateOffset(months=26))
        url = (f"https://api.e-stat.go.jp/rest/3.0/app/json/getStatsData?appId={os.getenv('ESTAT_APP_ID')}&statsDataId={ESTAT_RETAIL_PRICE_TABLE}"
               f"&cdArea={ESTAT_TOKYO_AREA}&cdCat02={','.join(item_codes)}&cdTimeFrom={start.year}00{start.month:02d}{start.month:02d}")
        response_json = api_get_call(url, sleeper=10, retries=5)
        result = response_json['GET_STATS_DATA']
        if result['RESULT']['STATUS'] != 0:
            raise ValueError(f"e-Stat error: {result['RESULT']['ERROR_MSG']}")
        out = {}
        for v in result['STATISTICAL_DATA']['DATA_INF']['VALUE']:
            t = v['@time']  ## e.g. 2026000808 -> 2026-08
            out.setdefault(v['@cat02'], {})[pd.Timestamp(f"{t[:4]}-{t[6:8]}-01")] = float(v['$'])
        return {k: pd.Series(d).sort_index() for k, d in out.items()}

    ## (price series, sources) for an item config
    def get_item_price_series(self, cfg:dict, estat_prices:dict):
        if cfg['method'] == 'fred':
            return self.get_fred_series(cfg['series']), [SOURCES[cfg['source']]]
        if cfg['method'] == 'estat':
            return estat_prices[cfg['series']], [SOURCES['estat']]

        ## anchor x index
        index_cfg, anchor_cfg = cfg['index'], cfg['anchor']
        if 'eurostat' in index_cfg:
            index, sources = self.get_eurostat_hicp(index_cfg['eurostat']), [SOURCES['eurostat']]
        else:
            index, sources = self.get_ons_series(index_cfg['ons']), [SOURCES['ons']]
        month = pd.Timestamp(anchor_cfg['month'])
        if 'insee' in anchor_cfg:
            anchor, anchor_source = self.get_insee_series(anchor_cfg['insee'])[month], SOURCES['insee']
        elif 'ons' in anchor_cfg:
            anchor, anchor_source = self.get_ons_series(anchor_cfg['ons'])[month] * anchor_cfg.get('scale', 1), SOURCES['ons']
        else:
            anchor, anchor_source = anchor_cfg['value'], SOURCES[anchor_cfg['source']]
        if anchor_source not in sources:
            sources = [anchor_source] + sources
        ## index at the anchor month (or the latest month before it, if not published yet)
        return anchor * index / index[index.index <= month].iloc[-1], sources

    ## USD per unit of local currency, daily
    def get_fx_usd_per_unit(self, currency:str):
        series_id, inverted = FX_SERIES[currency]
        fx = self.get_fred_series(series_id)
        return 1 / fx if inverted else fx

    ## 'What one ETH actually buys' per region: units of everyday goods per ETH today vs. a year ago, and what drove the change.
    ## The change in units is split exactly into: ETH price effect (at year-ago shop prices) + shop price effect (inflation, at today's ETH price).
    ## ETH and gold are converted to local currency with daily FX, so the ETH price effect includes currency moves.
    def get_purchasing_power_regions(self, eth_usd_now:float, eth_usd_year_ago:float, year_ago):
        import yfinance as yf
        gold = yf.Ticker('GC=F').history(period='13mo', interval='1d')['Close']
        gold.index = gold.index.tz_localize(None)
        gold_ago = gold[gold.index <= gold.index[-1] - pd.DateOffset(years=1)]

        estat_codes = [c['series'] for c in PURCHASING_POWER_REGIONS['japan']['items'].values()]
        estat_prices = self.get_estat_prices(estat_codes)

        regions = {}
        for region_key, region in PURCHASING_POWER_REGIONS.items():
            currency = region['currency']
            if currency == 'USD':
                fx_now, fx_ago, fx_sources = 1.0, 1.0, []
            else:
                fx = self.get_fx_usd_per_unit(currency)
                fx_now, fx_ago, fx_sources = float(fx.iloc[-1]), float(fx[fx.index <= pd.Timestamp(year_ago)].iloc[-1]), [SOURCES['fred_fx']]
            eth_now, eth_ago = eth_usd_now / fx_now, eth_usd_year_ago / fx_ago

            prices = {}
            for item_key, cfg in region['items'].items():
                series, sources = self.get_item_price_series(cfg, estat_prices)
                ## convert to the standard unit
                series = series.dropna() * cfg['factor']
                latest = series.index[-1]
                ago = series[series.index <= latest - pd.DateOffset(years=1)]
                prices[item_key] = (float(series.iloc[-1]), float(ago.iloc[-1]), latest, cfg['source_unit'], sources)
            prices['gold'] = (float(gold.iloc[-1]) / TROY_OZ_IN_GRAMS / fx_now, float(gold_ago.iloc[-1]) / TROY_OZ_IN_GRAMS / fx_ago,
                              gold.index[-1], 'gram (gold futures price per troy ounce / 31.1035)', [SOURCES['yahoo']] + fx_sources)

            items = []
            for key, (p_now, p_ago, as_of, source_unit, sources) in prices.items():
                units_now, units_ago = eth_now / p_now, eth_ago / p_ago
                items.append({
                    'key': key, 'name': PURCHASING_POWER_ITEM_NAMES[key], 'unit': STANDARD_UNITS[key], 'source_unit': source_unit,
                    'units_per_eth': units_now,
                    'units_per_eth_year_ago': units_ago,
                    'change_pct': units_now / units_ago - 1,
                    'change_units': {
                        'eth_price': eth_now / p_ago - units_ago,
                        'shop_price': units_now - eth_now / p_ago,
                    },
                    'price': p_now, 'price_year_ago': p_ago, 'price_as_of': as_of.strftime('%Y-%m-%d'),
                    'sources': sources,
                })
            regions[region_key] = {
                'name': region['name'], 'currency': currency,
                'eth_price': eth_now, 'eth_price_year_ago': eth_ago,
                'items': items,
            }
        return regions

    def create_purchasing_power_json(self):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        end_hour = pd.Timestamp(now).floor('h') - pd.Timedelta(hours=1)
        df_hourly = self.get_hourly_data()
        eth_now = float(self.value_at(df_hourly['price_usd'], end_hour))
        year_ago = (pd.Timestamp(now.date()) - pd.DateOffset(years=1)).date()
        eth_year_ago = float(self.db_connector.execute_query(f"""
            SELECT value FROM public.fact_kpis WHERE origin_key = 'ethereum' AND metric_key = 'price_usd' AND "date" <= '{year_ago}' ORDER BY "date" DESC LIMIT 1
        """, load_df=True)['value'].iloc[0])

        details_dict = {
            'data': {
                'year_ago_date': str(year_ago),
                'regions': self.get_purchasing_power_regions(eth_now, eth_year_ago, year_ago),
                'eth_price_sources': [SOURCES['coingecko']],
            },
            'last_updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        }
        details_dict = fix_dict_nan(details_dict, 'eth_asset/purchasing_power')

        write_local_section(self.s3_bucket, self.api_version, 'purchasing_power', details_dict)
        print('DONE -- ETH asset purchasing power json export done')
        return details_dict

    ## 'The supply, in one picture': weekly issuance to stakers (tap) vs. burn (drain) for last week and historical scenarios.
    ## Issuance in scenarios is always today's (it depends on the amount staked, not on activity), burn comes from the scenario week.
    def create_supply_flow_json(self):
        import numpy as np
        df = self.db_connector.execute_query("""
            SELECT "date", metric_key, value FROM public.eim_fact
            WHERE origin_key = 'ethereum' AND metric_key IN ('eth_burned_eth', 'staking_rewards_cl_eth') AND "date" >= '2021-08-05'
        """, load_df=True)
        df['date'] = pd.to_datetime(df['date'])
        p = df.pivot_table(index='date', columns='metric_key', values='value', aggfunc='last').sort_index()
        today = pd.Timestamp(datetime.now(timezone.utc).date())
        p = p[p.index < today]

        ## last week = last 7 complete days with both issuance and burn
        last = p.dropna().tail(7)
        issuance_week = float(last['staking_rewards_cl_eth'].sum())

        ## supply level at the end of the last complete day (hourly supply at today 00:00 UTC, or the latest before);
        ## daily water levels are reconstructed backwards from issuance - burn
        supply = self.db_connector.execute_query(f"""
            SELECT "timestamp", value FROM public.fact_kpis_granular
            WHERE origin_key = 'ethereum' AND metric_key = 'eth_supply_eth' AND granularity = 'hourly' AND "timestamp" <= '{today}'
            ORDER BY "timestamp" DESC LIMIT 1
        """, load_df=True).iloc[0]
        supply_now = float(supply['value'])
        net_daily = last['staking_rewards_cl_eth'] - last['eth_burned_eth']
        supply_end_of_day = supply_now - net_daily[::-1].cumsum()[::-1].shift(-1, fill_value=0)

        ## scenario weeks from rolling 7-day burn
        weekly_burn = p['eth_burned_eth'].rolling(7).sum().dropna()
        ## real week whose burn is closest to the given quantile within the window
        def week_closest_to(window, q):
            end = (window - window.quantile(q)).abs().idxmin()
            return end, float(window[end])
        quiet_end, quiet_burn = week_closest_to(weekly_burn[weekly_burn.index >= weekly_burn.index.max() - pd.Timedelta(days=SUPPLY_FLOW_QUIET_LOOKBACK_DAYS)], SUPPLY_FLOW_QUIET_QUANTILE)
        busy_end, busy_burn = week_closest_to(weekly_burn[weekly_burn.index >= pd.Timestamp(SUPPLY_FLOW_BUSY_SINCE) + pd.Timedelta(days=6)], SUPPLY_FLOW_BUSY_QUANTILE)
        mania_end, mania_burn = weekly_burn.idxmax(), float(weekly_burn.max())

        ## network demand slider: log scale, quiet week = 0, mania = 1
        log_lo, log_hi = np.log(quiet_burn), np.log(mania_burn)
        demand_level = lambda burn: float(min(max((np.log(max(burn, 1e-9)) - log_lo) / (log_hi - log_lo), 0), 1))

        def scenario(key, name, burn, start, end, description, daily=None):
            net = issuance_week - burn
            out = {
                'key': key, 'name': name,
                'period': {'start': start.strftime('%Y-%m-%d'), 'end': end.strftime('%Y-%m-%d')},
                'issued_per_week': issuance_week, 'burned_per_week': burn, 'net_per_week': net,
                'net_annual_pct': net * 365 / 7 / supply_now,
                'burned_share_of_issuance': burn / issuance_week,
                'demand_level': demand_level(burn),
                'description': description,
            }
            if daily is not None:
                out['daily'] = daily
            return out

        unix = (last.index - pd.Timestamp('1970-01-01')) // pd.Timedelta(milliseconds=1)
        daily = {
            'types': ['unix', 'issued', 'burned', 'supply_end_of_day'],
            'data': [[int(u), float(i), float(b), float(sv)] for u, i, b, sv in zip(unix, last['staking_rewards_cl_eth'], last['eth_burned_eth'], supply_end_of_day)],
        }
        last_issued, last_burned = float(last['staking_rewards_cl_eth'].sum()), float(last['eth_burned_eth'].sum())
        scenarios = [
            scenario('last_week', 'Last week', last_burned, last.index[0], last.index[-1], 'Actual issuance and burn of the last 7 complete days.', daily),
            scenario('quiet_week', 'Quiet week', quiet_burn, quiet_end - pd.Timedelta(days=6), quiet_end,
                     f'A real week with little network activity: {int(SUPPLY_FLOW_QUIET_QUANTILE*100)}th percentile of weekly burn over the last year, with today\'s issuance.'),
            scenario('busy_week', 'Busy week', busy_burn, busy_end - pd.Timedelta(days=6), busy_end,
                     f'A real week with high network activity: {int(SUPPLY_FLOW_BUSY_QUANTILE*100)}th percentile of weekly burn since the Merge (Sep 2022), with today\'s issuance.'),
            scenario('mania', 'Mania', mania_burn, mania_end - pd.Timedelta(days=6), mania_end,
                     "The highest weekly burn since EIP-1559 (Aug 2021), with today's issuance."),
        ]
        scenarios[0]['issued_per_week'] = last_issued  ## identical to issuance_week, kept explicit for last week

        details_dict = {
            'data': {
                'supply': {'value': supply_now, 'as_of': pd.Timestamp(supply['timestamp']).strftime('%Y-%m-%d %H:%M')},
                'break_even_burn_per_week': issuance_week,
                'break_even_demand_level': demand_level(issuance_week),
                'scenarios': scenarios,
                'sources': [SOURCES['ethsupply'], SOURCES['growthepie'], SOURCES['ultrasound']],
            },
            'last_updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        }
        details_dict = fix_dict_nan(details_dict, 'eth_asset/supply_flow')

        write_local_section(self.s3_bucket, self.api_version, 'supply_flow', details_dict)
        print('DONE -- ETH asset supply flow json export done')
        return details_dict

    ## ETH used as collateral (latest daily snapshot), partitioned into lending / liquid staking / DEX without overlap
    def get_collateral(self, price_usd:float):
        df = self.db_connector.execute_query("""
            SELECT metric_key, "date", value
            FROM public.eim_fact
            WHERE origin_key = 'ethereum'
                AND metric_key IN ('collateral_lending_eth', 'collateral_liquid_staking_eth', 'collateral_dex_eth')
                AND "date" = (SELECT MAX("date") FROM public.eim_fact WHERE origin_key = 'ethereum' AND metric_key = 'collateral_lending_eth')
        """, load_df=True)
        values = {r['metric_key'].replace('collateral_', '').replace('_eth', ''): float(r['value']) for r in df.to_dict(orient='records')}
        total = sum(values.values())
        return {
            'name': 'ETH Posted as Collateral',
            'as_of': str(df['date'].max()),
            'total_eth': total,
            'total_usd': total * price_usd,
            'breakdown': {k: {'eth': v, 'usd': v * price_usd, 'share': v / total} for k, v in sorted(values.items(), key=lambda kv: -kv[1])},
            'sources': [SOURCES['defillama'], SOURCES['growthepie']],
        }

    ## daily ETH paid for network usage (fees) on Ethereum + L2s with ETH as gas token
    def get_gas_paid(self):
        from src.main_config import get_main_config
        chains = [c.origin_key for c in get_main_config() if c.metadata_gas_token == 'ETH' and c.api_in_main and c.api_deployment_flag == 'PROD']
        ok_string = "', '".join(chains)
        df = self.db_connector.execute_query(f"""
            SELECT "date", origin_key, metric_key, value
            FROM public.fact_kpis
            WHERE metric_key IN ('fees_paid_eth', 'fees_paid_usd')
                AND origin_key IN ('{ok_string}')
                AND "date" >= current_date - interval '{GAS_PAID_DAYS} days'
                AND "date" < current_date
        """, load_df=True)
        df['date'] = pd.to_datetime(df['date'])
        ## drop days where fees_paid_usd isn't there yet (metrics_sql eth_to_usd runs ~03:30, after fees_paid_eth lands)
        daily = df.pivot_table(index='date', columns='metric_key', values='value', aggfunc='sum').sort_index().dropna()
        ## chains counted = chains with fees in the last 7 days
        active = sorted(df[(df['metric_key'] == 'fees_paid_eth') & (df['date'] >= df['date'].max() - pd.Timedelta(days=6)) & (df['value'] > 0)]['origin_key'].unique())
        unix = (daily.index - pd.Timestamp('1970-01-01')) // pd.Timedelta(milliseconds=1)
        return {
            'name': 'ETH Paid as Gas',
            'unit': 'eth',
            'granularity': 'daily',
            'types': ['unix', 'eth', 'usd'],
            'data': [[int(u), float(e), float(d)] for u, e, d in zip(unix, daily['fees_paid_eth'], daily['fees_paid_usd'])],
            'chains': {'count_l1': int('ethereum' in active), 'count_l2': len([c for c in active if c != 'ethereum']), 'origin_keys': active},
            'sources': [SOURCES['growthepie']],
        }

    def create_properties_json(self):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        end_hour = pd.Timestamp(now).floor('h') - pd.Timedelta(hours=1)
        end_day = pd.Timestamp(now.date()) - pd.Timedelta(days=1)
        df_daily = self.prepare_daily(self.get_daily_data(), end_day)
        df_hourly = self.prepare_hourly(self.get_hourly_data(), df_daily, end_hour)
        price_usd = self.value_at(df_hourly['price_usd'], end_hour)

        details_dict = {
            'data': {
                'yield_breakdown': self.get_yield_breakdown(df_hourly, end_hour),
                'scarcity': self.get_scarcity(df_hourly, df_daily, end_hour),
                'collateral': self.get_collateral(price_usd),
                'gas_paid': self.get_gas_paid(),
            },
            'last_updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        }
        details_dict = fix_dict_nan(details_dict, 'eth_asset/properties')

        write_local_section(self.s3_bucket, self.api_version, 'properties', details_dict)
        print('DONE -- ETH asset properties json export done')
        return details_dict

    ## ----------------- Market caps ('How big is it really' section) --------------------

    def get_crypto_market_caps(self):
        import os
        import re
        response_json = api_get_call(f'https://pro-api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page={MCAP_CRYPTO_TOP_N}', sleeper=10, retries=5,
                                     header={'x-cg-pro-api-key': os.getenv('COINGECKO_API')})
        if not response_json or not {'ethereum', 'bitcoin'} <= {c['id'] for c in response_json}:
            raise ValueError("ETH/BTC missing in CoinGecko market data")
        return [{
            'key': re.sub(r'[^a-z0-9]+', '_', c['symbol'].lower()).strip('_'), 'name': c['name'], 'category': 'crypto',
            'market_cap_usd': float(c['market_cap']), 'logo_source_url': c['image'],
            'sources': [SOURCES['coingecko']],
        } for c in response_json]

    ## daily ETH/BTC market cap ratio (complete days, values as of 00:00 UTC like CoinGecko), current = live ratio from /coins/markets
    def get_eth_btc_ratio(self, crypto_items:list):
        import yfinance as yf
        today = pd.Timestamp(datetime.now(timezone.utc).date())
        start = today - pd.Timedelta(days=ETH_BTC_RATIO_DAYS)

        eth = self.db_connector.execute_query(f"""
            SELECT "date", value FROM public.fact_kpis
            WHERE origin_key = 'ethereum' AND metric_key = 'market_cap_usd' AND "date" >= '{start.date()}'
        """, load_df=True)
        eth = pd.Series(eth['value'].values, index=pd.to_datetime(eth['date']))

        ## BTC close of day D-1 = value at 00:00 UTC of day D (CoinGecko convention)
        btc_price = yf.Ticker('BTC-USD').history(period='7y', interval='1d')['Close']
        btc_price.index = btc_price.index.tz_localize(None).normalize() + pd.Timedelta(days=1)
        response_json = api_get_call(BTC_SUPPLY_URL, sleeper=10, retries=5)
        if not response_json or not response_json.get('values'):
            raise ValueError("No BTC supply returned from blockchain.com")
        supply = pd.Series({pd.Timestamp(v['x'], unit='s').normalize(): v['y'] for v in response_json['values']})
        supply = supply[~supply.index.duplicated(keep='last')].sort_index()
        supply = supply.reindex(supply.index.union(btc_price.index)).interpolate(method='time').ffill()
        btc = (btc_price * supply.reindex(btc_price.index)).dropna()

        df = pd.DataFrame({'eth': eth, 'btc': btc}).dropna()
        df = df[(df.index >= start) & (df.index < today) & (df['btc'] > 0)]
        ratio = (df['eth'] / df['btc']).sort_index()

        live = {i['key']: i['market_cap_usd'] for i in crypto_items}
        unix = (ratio.index - pd.Timestamp('1970-01-01')) // pd.Timedelta(milliseconds=1)
        return {
            'name': 'ETH/BTC Market Cap Ratio',
            'unit': 'ratio',
            'granularity': 'daily',
            'current': live['eth'] / live['btc'],
            'types': ['unix', 'value'],
            'data': [[int(u), float(v)] for u, v in zip(unix, ratio.values)],
            'sources': [SOURCES['coingecko'], SOURCES['yahoo'], SOURCES['blockchain_com']],
        }

    ## top companies worldwide by market cap (USD) from the CompaniesMarketCap csv
    def get_equity_market_caps(self, eth_market_cap:float):
        import io, re, requests
        r = requests.get(CMC_COMPANIES_CSV_URL, headers=HTTP_HEADERS, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text)).sort_values('marketcap', ascending=False)
        n = int((df['marketcap'] > eth_market_cap).sum()) + MCAP_EQUITY_BELOW_ETH
        if len(df) < n:
            raise ValueError(f"Only {len(df)} companies returned from CompaniesMarketCap")
        df = df.head(n)
        return [{
            'key': re.sub(r'[^a-z0-9]+', '_', str(row['Symbol']).lower()).strip('_'), 'name': row['Name'], 'category': 'equity', 'ticker': row['Symbol'],
            'country': row['country'], 'market_cap_usd': float(row['marketcap']), 'logo_source_url': CMC_LOGO_URL.format(row['Symbol']),
            'sources': [SOURCES['companiesmarketcap']],
        } for _, row in df.iterrows()]

    ## commodities listed on the CompaniesMarketCap assets ranking (gold, silver, platinum) + oil (proved reserves x Brent)
    def get_commodity_market_caps(self):
        import io, re, requests
        import yfinance as yf

        html = requests.get(CMC_ASSETS_URL, headers=HTTP_HEADERS, timeout=60)
        html.raise_for_status()
        items = []
        for row in re.findall(r'<tr.*?</tr>', html.text, re.S):
            logo = re.search(r'src="/img/company-logos/64/([^"]+?)\.png"', row)
            ## commodities have logo ids ending with .XM (crypto: .X)
            if not logo or not logo.group(1).endswith('.XM'):
                continue
            cells = [c.strip() for c in re.sub(r'<[^>]+>', '|', row).split('|') if c.strip()]
            ## cells: rank, name, symbol, market cap, price, ...
            mcap = re.match(r'\$([\d.,]+)\s*([TBM])', cells[3])
            mult = {'T': 1e12, 'B': 1e9, 'M': 1e6}[mcap.group(2)]
            items.append({
                'key': cells[1].lower(), 'name': cells[1], 'category': 'commodity',
                'market_cap_usd': float(mcap.group(1).replace(',', '')) * mult, 'logo_source_url': CMC_LOGO_URL.format(logo.group(1)),
                'method': 'Estimated above-ground stock x spot price', 'sources': [SOURCES['companiesmarketcap']],
            })
        if not any(i['key'] == 'gold' for i in items):
            raise ValueError("Gold not found on CompaniesMarketCap assets ranking")

        oil = api_get_call(OIL_RESERVES_URL, sleeper=10, retries=5, as_json=False)
        oil = pd.read_csv(io.StringIO(oil if isinstance(oil, str) else oil.text))
        oil = oil[oil['entity'] == 'World'].sort_values('year').iloc[-1]
        oil_barrels = float(oil.iloc[-1]) * BARRELS_PER_M3
        brent = float(yf.Ticker('BZ=F').fast_info.get('lastPrice'))
        items.append({
            'key': 'oil', 'name': 'Oil', 'category': 'commodity', 'market_cap_usd': oil_barrels * brent, 'logo_source_url': OIL_LOGO_URL,
            'method': f"World proved reserves ({oil_barrels/1e9:,.0f}B barrels, {int(oil['year'])}) x Brent price", 'sources': [SOURCES['eia'], SOURCES['yahoo']],
        })
        return items

    ## downloads missing logos once and serves them from our CDN, returns the public logo url
    def ensure_logo(self, key:str, source_url:str):
        import os, requests, tempfile
        from src.misc.helper_functions import upload_image_to_cf_s3
        s3_path = f'{self.api_version}/eth_asset/logos/{key}'
        public_url = f'https://api.growthepie.com/{s3_path}.png'
        if self.s3_bucket is None:
            return public_url
        if requests.head(public_url, timeout=20).status_code == 200:
            return public_url
        r = requests.get(source_url, headers=HTTP_HEADERS, timeout=30)
        if r.status_code != 200 or not r.headers.get('content-type', '').startswith('image/png'):
            print(f"...logo for {key} not available at {source_url}")
            return None
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as f:
            f.write(r.content)
        upload_image_to_cf_s3(self.s3_bucket, s3_path, f.name, self.cf_distribution_id, 'png', invalidate=False)
        os.remove(f.name)
        return public_url

    def create_market_caps_json(self):
        crypto = self.get_crypto_market_caps()
        eth_btc_ratio = self.get_eth_btc_ratio(crypto)
        eth_market_cap = next(i['market_cap_usd'] for i in crypto if i['key'] == 'eth')
        items = crypto + self.get_equity_market_caps(eth_market_cap) + self.get_commodity_market_caps()
        for item in items:
            item['logo'] = self.ensure_logo(item['key'], item.pop('logo_source_url'))

        eth = next(i for i in items if i['key'] == 'eth')
        details_dict = {
            'data': {
                'eth': {'market_cap_usd': eth['market_cap_usd'], 'logo': eth['logo'], 'sources': eth['sources']},
                'eth_btc_ratio': eth_btc_ratio,
                'categories': {
                    cat: sorted([i for i in items if i['category'] == cat], key=lambda x: -x['market_cap_usd'])
                    for cat in ['crypto', 'commodity', 'equity']
                },
            },
            'last_updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        }
        details_dict = fix_dict_nan(details_dict, 'eth_asset/market_caps')

        write_local_section(self.s3_bucket, self.api_version, 'market_caps', details_dict)
        print('DONE -- ETH asset market caps json export done')
        return details_dict

    ## bundle = data.<section> for each section created in this run (passed in by the DAG), plus each section's last update.
    ## Sections that failed or weren't created in this run keep their data from the currently published bundle.
    def create_bundle_json(self, bundle:str, sections:dict):
        previous = {}
        try:
            import boto3
            s3 = boto3.client('s3', aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'), aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'))
            previous = json.loads(s3.get_object(Bucket=self.s3_bucket, Key=f'{self.api_version}/eth_asset/{bundle}.json')['Body'].read())
        except Exception as e:
            print(f"...no previously published {bundle} bundle available: {e}")

        data, updated, reused = {}, {}, []
        for section in ETH_ASSET_BUNDLES[bundle]:
            content = sections.get(section)
            if content:
                data[section] = content['data']
                updated[section] = content.get('last_updated_utc')
            elif section in previous.get('data', {}):
                data[section] = previous['data'][section]
                updated[section] = previous.get('sections_last_updated_utc', {}).get(section)
                reused.append(section)
            else:
                print(f"...section {section} not available for bundle {bundle}")
        if not data or len(reused) == len(data):
            raise ValueError(f"No new sections for bundle {bundle}")

        details_dict = {
            'data': data,
            'sections_last_updated_utc': updated,
            'last_updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        }
        upload_json_to_cf_s3(self.s3_bucket, f'{self.api_version}/eth_asset/{bundle}', details_dict, self.cf_distribution_id, cache_control=ETH_ASSET_CACHE_CONTROL, invalidate=False)
        print(f"DONE -- ETH asset bundle {bundle} export done ({len(data)} sections{', reused from previous bundle: ' + ', '.join(reused) if reused else ''})")
        return details_dict

    def create_kpis_json(self):
        df_hourly = self.get_hourly_data()
        df_daily = self.get_daily_data()

        details_dict = {
            'data': {
                'kpis': self.create_kpi_dict(df_hourly, df_daily),
            },
            'last_updated_utc': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
        }
        details_dict = fix_dict_nan(details_dict, 'eth_asset/kpis')

        write_local_section(self.s3_bucket, self.api_version, 'kpis', details_dict)
        print('DONE -- ETH asset KPIs json export done')
        return details_dict
