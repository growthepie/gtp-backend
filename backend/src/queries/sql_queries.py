import sys
import getpass
sys_user = getpass.getuser()
sys.path.append(f"/home/{sys_user}/gtp/backend/")

from jinja2 import Environment, FileSystemLoader, StrictUndefined
if sys_user == 'ubuntu':
        env = Environment(loader=FileSystemLoader(f'/home/{sys_user}/gtp/backend/src/queries/postgres'), undefined=StrictUndefined)
else:
        env = Environment(loader=FileSystemLoader('src/queries/postgres'), undefined=StrictUndefined)

class SQLQuery():
    def __init__(self, jinja_path: str, metric_key: str, origin_key: str, query_parameters: dict = None, currency_dependent:bool = True, run_hourly:bool = False):
        self.template = env.get_template(jinja_path)
        self.metric_key = metric_key
        self.origin_key = origin_key
        self.query_parameters = query_parameters if query_parameters is not None else {}
        self.query_parameters['origin_key'] = origin_key
        self.currency_dependent = currency_dependent ## if false, the query can in parellel to the currency queries 
        self.run_hourly = run_hourly ## if true, the query will be run in the hourly metrics dag instead of the daily one

def standard_evm_queries(origin_key: str): ## op-stack and others
     return [
                SQLQuery(metric_key = "txcount_comparison", origin_key = origin_key, jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
                ,SQLQuery(metric_key = "txcount", origin_key = origin_key, jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
                ,SQLQuery(metric_key = "txcount_plain", origin_key = origin_key, jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "daa", origin_key = origin_key, jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "waa", origin_key = origin_key, jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "maa", origin_key = origin_key, jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "qaa", origin_key = origin_key, jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "aa_last7d", origin_key = origin_key, jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
                ,SQLQuery(metric_key = "aa_last30d", origin_key = origin_key, jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
                ,SQLQuery(metric_key = "gas_per_second", origin_key = origin_key, jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = origin_key, jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = origin_key, jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
                ,SQLQuery(metric_key = "fees_paid_eth", origin_key = origin_key, jinja_path='chain_metrics/select_fees_paid.sql.j2')
                ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = origin_key, jinja_path='chain_metrics/select_txcosts_median.sql.j2')
                ## hourly
                ,SQLQuery(metric_key = "txcount", origin_key = origin_key, jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False, run_hourly = True)
                ,SQLQuery(metric_key = "daa", origin_key = origin_key, jinja_path='chain_metrics/select_aa_hourly.sql.j2', currency_dependent = False, run_hourly = True)
                ,SQLQuery(metric_key = "gas_per_second", origin_key = origin_key, jinja_path='chain_metrics/select_gas_per_second_hourly.sql.j2', currency_dependent = False, run_hourly = True)
                ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = origin_key, jinja_path='chain_metrics/select_txcosts_median_hourly.sql.j2', run_hourly = True)
                ,SQLQuery(metric_key = "fees_paid_eth", origin_key = origin_key, jinja_path='chain_metrics/select_fees_paid_hourly.sql.j2', run_hourly = True)
        ]

## Queries have default values defined in the jinja templates. These can either be overwritten here or later in adater_sql.py
sql_queries = [
        # Multi-chain
        SQLQuery(metric_key = "user_base_weekly", origin_key = "multi", jinja_path='chain_metrics/select_user_base_weekly.sql.j2', currency_dependent = False)
        
        # --- Layer 1 ---
        # Ethereum
        ,SQLQuery(metric_key = "txcount_comparison", origin_key = "ethereum", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "ethereum", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount_plain", origin_key = "ethereum", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "ethereum", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "ethereum", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "ethereum", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "ethereum", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "ethereum", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "ethereum", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "ethereum", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "ethereum", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "ethereum", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "ethereum", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "ethereum", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        ## hourly
        ,SQLQuery(metric_key = "txcount", origin_key = "ethereum", jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "daa", origin_key = "ethereum", jinja_path='chain_metrics/select_aa_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "ethereum", jinja_path='chain_metrics/select_gas_per_second_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "ethereum", jinja_path='chain_metrics/select_txcosts_median_hourly.sql.j2', query_parameters={}, run_hourly = True)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "ethereum", jinja_path='chain_metrics/select_fees_paid_hourly.sql.j2', query_parameters={}, run_hourly = True)
        
        ,SQLQuery(metric_key = "txcount_type4", origin_key = "ethereum", jinja_path='chain_metrics/select_txcount_type4.sql.j2', currency_dependent = False)

        # --- DA Layers ---
        # Celestia
        ,SQLQuery(metric_key = "da_data_posted_bytes", origin_key = "da_celestia", jinja_path='da_metrics/celestia_da_data_posted_bytes.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "da_unique_blob_producers", origin_key = "da_celestia", jinja_path='da_metrics/celestia_da_unique_blob_producers.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "da_blob_count", origin_key = "da_celestia", jinja_path='da_metrics/celestia_da_blob_count.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "da_fees_eth", origin_key = "da_celestia", jinja_path='da_metrics/celestia_da_fees_eth.sql.j2')

        # --- Layer 2s ---
        # Arbitrum Orbit
        # Different filter col on txcounts (gas_used instead of gas_price)
        ## Arbitrum (some data pulled via Dune)
        ,SQLQuery(metric_key = "txcount_comparison", origin_key = "arbitrum", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "arbitrum", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount_plain", origin_key = "arbitrum", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "arbitrum", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "arbitrum", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "arbitrum", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "arbitrum", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "arbitrum", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "arbitrum", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "arbitrum", jinja_path='chain_metrics/custom/orbit_select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "arbitrum", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "arbitrum", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_base_eth", origin_key = "arbitrum", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "arbitrum", jinja_path='chain_metrics/select_fees_paid_combined.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "arbitrum", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        ,SQLQuery(metric_key = "txcount_type4", origin_key = "arbitrum", jinja_path='chain_metrics/select_txcount_type4.sql.j2', currency_dependent = False)
        ## hourly 
        ,SQLQuery(metric_key = "txcount", origin_key = "arbitrum", jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "daa", origin_key = "arbitrum", jinja_path='chain_metrics/select_aa_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "arbitrum", jinja_path='chain_metrics/select_gas_per_second_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "arbitrum", jinja_path='chain_metrics/select_txcosts_median_hourly.sql.j2', run_hourly = True)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "arbitrum", jinja_path='chain_metrics/select_fees_paid_hourly.sql.j2', run_hourly = True)

        ## Arbitrum Nova
        ,SQLQuery(metric_key = "txcount_comparison", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount_plain", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "arbitrum_nova", jinja_path='chain_metrics/custom/orbit_select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        ## hourly
        ,SQLQuery(metric_key = "txcount", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "daa", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_aa_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_gas_per_second_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_txcosts_median_hourly.sql.j2', run_hourly = True)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "arbitrum_nova", jinja_path='chain_metrics/select_fees_paid_hourly.sql.j2', run_hourly = True)
        
        # ,SQLQuery(metric_key = "txcount_comparison", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        # ,SQLQuery(metric_key = "txcount", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        # ,SQLQuery(metric_key = "txcount_plain", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "daa", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "waa", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "maa", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "qaa", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "aa_last7d", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        # ,SQLQuery(metric_key = "aa_last30d", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        # ,SQLQuery(metric_key = "gas_per_second", origin_key = "testnet_robinhood", jinja_path='chain_metrics/custom/orbit_select_gas_per_second.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        # ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        ## hourly
        ,SQLQuery(metric_key = "txcount", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "daa", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_aa_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_gas_per_second_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_txcosts_median_hourly.sql.j2', run_hourly = True)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "testnet_robinhood", jinja_path='chain_metrics/select_fees_paid_hourly.sql.j2', run_hourly = True)
        
        # SUPERCHAIN
        ## OP Mainnet
        ,*standard_evm_queries("optimism")
        ,SQLQuery(metric_key = "txcount_type4", origin_key = "optimism", jinja_path='chain_metrics/select_txcount_type4.sql.j2', query_parameters={}, currency_dependent = False)
        ## Base
        ,*standard_evm_queries("base")
        ,SQLQuery(metric_key = "txcount_type4", origin_key = "base", jinja_path='chain_metrics/select_txcount_type4.sql.j2', query_parameters={}, currency_dependent = False)
        ## Zora
        # ,*standard_evm_queries("zora")  
        ## Mode
        ,*standard_evm_queries("mode")
        ## Worldchain
        ,*standard_evm_queries("worldchain")
        #,SQLQuery(metric_key = "txcount_type4", origin_key = "worldchain", jinja_path='chain_metrics/select_txcount_type4.sql.j2', query_parameters={}, currency_dependent = False)
        # ## Mint
        # ,*standard_evm_queries("mint")
        ## Fraxtal
        ,*standard_evm_queries("fraxtal")
        ## INK
        ,*standard_evm_queries("ink")
        ## Soneium
        ,*standard_evm_queries("soneium")
        #,SQLQuery(metric_key = "txcount_type4", origin_key = "soneium", jinja_path='chain_metrics/select_txcount_type4.sql.j2', query_parameters={}, currency_dependent = False)

        ## MegaETH - only AA related queries
        ,SQLQuery(metric_key = "daa", origin_key = "megaeth", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "megaeth", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "megaeth", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "megaeth", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "megaeth", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "megaeth", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "megaeth", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "megaeth", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)

        ## Robinhood - only AA related queries
        ,SQLQuery(metric_key = "daa", origin_key = "robinhood", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "robinhood", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "robinhood", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "robinhood", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "robinhood", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "robinhood", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "robinhood", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "robinhood", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)

        ## Polygon PoS - only AA related queries
        ,SQLQuery(metric_key = "daa", origin_key = "polygon_pos", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "polygon_pos", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "polygon_pos", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "polygon_pos", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "polygon_pos", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "polygon_pos", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "polygon_pos", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "polygon_pos", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        
        ## Ronin - only AA related queries
        ,SQLQuery(metric_key = "daa", origin_key = "ronin", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "ronin", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "ronin", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "ronin", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "ronin", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "ronin", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "ronin", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "ronin", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        
        
        # Elastic Chain
        ## ZKsync Era
        ,*standard_evm_queries("zksync_era")

        # Polygon zkStack
        # ## Polygon zkEVM - standard but txcount_comparison is plain txcount
        # ,SQLQuery(metric_key = "txcount_comparison", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "txcount", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        # ,SQLQuery(metric_key = "txcount_plain", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "daa", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "waa", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "maa", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "qaa", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "aa_last7d", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        # ,SQLQuery(metric_key = "aa_last30d", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        # ,SQLQuery(metric_key = "gas_per_second", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        # ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "polygon_zkevm", jinja_path='chain_metrics/select_txcosts_median.sql.j2')

        # Others EVM
        ## Linea
        ,*standard_evm_queries("linea")
        ## Scroll
        ,*standard_evm_queries("scroll")
        ## Taiko
        ,*standard_evm_queries("taiko")
        ## Manta
        ,*standard_evm_queries("manta")
        ## Unichain
        ,*standard_evm_queries("unichain")
        ,SQLQuery(metric_key = "txcount_type4", origin_key = "unichain", jinja_path='chain_metrics/select_txcount_type4.sql.j2', query_parameters={}, currency_dependent = False)

        ,*standard_evm_queries("lisk")

        ## Zircuit
        ,SQLQuery(metric_key = "txcount_comparison", origin_key = "zircuit", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = 'zircuit', jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount_plain", origin_key = 'zircuit', jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = 'zircuit', jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "zircuit", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = 'zircuit', jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = 'zircuit', jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = 'zircuit', jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = 'zircuit', jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = 'zircuit', jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = 'zircuit', jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = 'zircuit', jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = 'zircuit', jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = 'zircuit', jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        ## hourly
        ,SQLQuery(metric_key = "txcount", origin_key = "zircuit", jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "daa", origin_key = "zircuit", jinja_path='chain_metrics/select_aa_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "zircuit", jinja_path='chain_metrics/select_gas_per_second_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "zircuit", jinja_path='chain_metrics/select_txcosts_median_hourly.sql.j2', run_hourly = True)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "zircuit", jinja_path='chain_metrics/select_fees_paid_hourly.sql.j2', run_hourly = True)

        # Others EVM Custom Gas Token
        ## Mantle (also custom gas query)
        ,SQLQuery(metric_key = "txcount_comparison", origin_key = "mantle", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "mantle", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount_plain", origin_key = "mantle", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "mantle", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "mantle", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "mantle", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "mantle", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "mantle", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "mantle", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "mantle", jinja_path='chain_metrics/custom/mantle_select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "mantle", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "mantle", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "mantle", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "mantle", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')
        ## hourly
        ,SQLQuery(metric_key = "txcount", origin_key = "mantle", jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "daa", origin_key = "mantle", jinja_path='chain_metrics/select_aa_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "mantle", jinja_path='chain_metrics/custom/mantle_select_gas_per_second_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "mantle", jinja_path='chain_metrics/select_txcosts_median_custom_gas_hourly.sql.j2', run_hourly = True)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "mantle", jinja_path='chain_metrics/select_fees_paid_custom_gas_hourly.sql.j2', run_hourly = True)

        ## Metis
        ,SQLQuery(metric_key = "txcount_comparison", origin_key = "metis", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "metis", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount_plain", origin_key = "metis", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "metis", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "metis", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "metis", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "metis", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "metis", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "metis", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "metis", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "metis", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "metis", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "metis", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "metis", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')
        ## hourly
        ,SQLQuery(metric_key = "txcount", origin_key = "metis", jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "daa", origin_key = "metis", jinja_path='chain_metrics/select_aa_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "metis", jinja_path='chain_metrics/select_txcosts_median_custom_gas_hourly.sql.j2', run_hourly = True)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "metis", jinja_path='chain_metrics/select_fees_paid_custom_gas_hourly.sql.j2', run_hourly = True)

        ## Gravity (Orbit tx filter, also custom gas query)
        ,SQLQuery(metric_key = "txcount_comparison", origin_key = "gravity", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "gravity", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount_plain", origin_key = "gravity", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "gravity", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "gravity", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "gravity", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "gravity", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "gravity", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "gravity", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "gravity", jinja_path='chain_metrics/custom/orbit_select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "gravity", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "gravity", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "gravity", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "gravity", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')
        ## hourly
        ,SQLQuery(metric_key = "txcount", origin_key = "gravity", jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "daa", origin_key = "gravity", jinja_path='chain_metrics/select_aa_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "gravity", jinja_path='chain_metrics/custom/orbit_select_gas_per_second_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "gravity", jinja_path='chain_metrics/select_txcosts_median_custom_gas_hourly.sql.j2', run_hourly = True)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "gravity", jinja_path='chain_metrics/select_fees_paid_custom_gas_hourly.sql.j2', run_hourly = True)
        
        ##Plume
        ,SQLQuery(metric_key = "txcount_comparison", origin_key = "plume", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "plume", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount_plain", origin_key = "plume", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "plume", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "plume", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "plume", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "plume", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "plume", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "plume", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "plume", jinja_path='chain_metrics/custom/orbit_select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "plume", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "plume", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "plume", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "plume", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')
        ## hourly
        ,SQLQuery(metric_key = "txcount", origin_key = "plume", jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "daa", origin_key = "plume", jinja_path='chain_metrics/select_aa_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "plume", jinja_path='chain_metrics/custom/orbit_select_gas_per_second_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "plume", jinja_path='chain_metrics/select_txcosts_median_custom_gas_hourly.sql.j2', run_hourly = True)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "plume", jinja_path='chain_metrics/select_fees_paid_custom_gas_hourly.sql.j2', run_hourly = True)

        ## Celo (op stack but CELO gas token and only data available start March 26th when L2 went live)
        ,SQLQuery(metric_key = "txcount_comparison", origin_key = "celo", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "celo", jinja_path='chain_metrics/select_txcount.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False)
        ,SQLQuery(metric_key = "txcount_plain", origin_key = "celo", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "daa", origin_key = "celo", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "waa", origin_key = "celo", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "maa", origin_key = "celo", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "qaa", origin_key = "celo", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last7d", origin_key = "celo", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        ,SQLQuery(metric_key = "aa_last30d", origin_key = "celo", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "celo", jinja_path='chain_metrics/select_gas_per_second.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "celo", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "celo", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "celo", jinja_path='chain_metrics/select_fees_paid_custom_gas.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "celo", jinja_path='chain_metrics/select_txcosts_median_custom_gas.sql.j2')
        ## hourly
        ,SQLQuery(metric_key = "txcount", origin_key = "celo", jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_price"}, currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "daa", origin_key = "celo", jinja_path='chain_metrics/select_aa_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "gas_per_second", origin_key = "celo", jinja_path='chain_metrics/select_gas_per_second_hourly.sql.j2', currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "celo", jinja_path='chain_metrics/select_txcosts_median_custom_gas_hourly.sql.j2', run_hourly = True)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "celo", jinja_path='chain_metrics/select_fees_paid_custom_gas_hourly.sql.j2', run_hourly = True)

        # Others Non-EVM
        ## Loopring
        # ,SQLQuery(metric_key = "txcount_comparison", origin_key = "loopring", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "txcount", origin_key = "loopring", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "txcount_plain", origin_key = "loopring", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "daa", origin_key = "loopring", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "waa", origin_key = "loopring", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "maa", origin_key = "loopring", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "qaa", origin_key = "loopring", jinja_path='chain_metrics/select_qaa.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "aa_last7d", origin_key = "loopring", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        # ,SQLQuery(metric_key = "aa_last30d", origin_key = "loopring", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        # ,SQLQuery(metric_key = "cca_last7d_exclusive", origin_key = "loopring", jinja_path='chain_metrics/select_cca_last7d.sql.j2', currency_dependent = False)
        # ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "loopring", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)

        ## Starknet
        ,SQLQuery(metric_key = "txcount_comparison", origin_key = "starknet", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "txcount", origin_key = "starknet", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "txcount_plain", origin_key = "starknet", jinja_path='chain_metrics/select_txcount_plain.sql.j2', currency_dependent = False)
        #,SQLQuery(metric_key = "daa", origin_key = "starknet", jinja_path='chain_metrics/select_daa.sql.j2', currency_dependent = False)
        #,SQLQuery(metric_key = "maa", origin_key = "starknet", jinja_path='chain_metrics/select_maa.sql.j2', currency_dependent = False)
        #,SQLQuery(metric_key = "aa_last7d", origin_key = "starknet", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2', query_parameters={"timerange" : 7}, currency_dependent = False)
        #,SQLQuery(metric_key = "aa_last30d", origin_key = "starknet", jinja_path='chain_metrics/select_aa_lastXXd.sql.j2',query_parameters={"timerange" : 30}, currency_dependent = False)
        #,SQLQuery(metric_key = "user_base_weekly", origin_key = "starknet", jinja_path='chain_metrics/select_waa.sql.j2', currency_dependent = False)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "starknet", jinja_path='chain_metrics/select_fees_paid.sql.j2')
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "starknet", jinja_path='chain_metrics/select_txcosts_median.sql.j2')
        ,SQLQuery(metric_key = "cca_weekly_exclusive", origin_key = "starknet", jinja_path='chain_metrics/select_cca_weekly.sql.j2', currency_dependent = False)
        ## hourly
        ,SQLQuery(metric_key = "txcount", origin_key = "starknet", jinja_path='chain_metrics/select_txcount_hourly.sql.j2', query_parameters={"filter_col" : "gas_used"}, currency_dependent = False, run_hourly = True)
        ,SQLQuery(metric_key = "txcosts_median_eth", origin_key = "starknet", jinja_path='chain_metrics/select_txcosts_median_hourly.sql.j2', run_hourly = True)
        ,SQLQuery(metric_key = "fees_paid_eth", origin_key = "starknet", jinja_path='chain_metrics/select_fees_paid_hourly.sql.j2', run_hourly = True)
]
