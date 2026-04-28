# Units
## Decimals: only relevant if value isn't aggregated
## When aggregated (starting >1k), we always show 2 decimals
## in case of ETH and decimals >6, show Gwei
## prefix and suffix should also show in axis
gtp_units = {
        'value': {
            'currency': False,
            'prefix': None,
            'suffix': None,
            'decimals': 0,
            'decimals_tooltip': 0,
            'agg': True,
            'agg_tooltip': True,
        },
        'usd': {
            'currency': True,
            'prefix': '$',
            'suffix': None,
            'decimals': 2,
            'decimals_tooltip': 2,
            'agg': True, 
            'agg_tooltip': False,
        },
        'eth': {
            'currency': True,
            'prefix': 'Ξ',
            'suffix': None,
            'decimals': 2,
            'decimals_tooltip': 2,
            'agg': True,
            'agg_tooltip': False,
        },
    }

# Sources
gtp_sources = {
        'l2beat': {
            'name': 'L2BEAT',
            'url': 'https://l2beat.com'
        },
        'defillama': {
            'name': 'DefiLlama',
            'url': 'https://defillama.com'
        },
        'growthepie': {
            'name': 'growthepie',
            'url': 'https://growthepie.com'
        },
        'coingecko': {
            'name': 'CoinGecko',
            'url': 'https://www.coingecko.com'
        },
        'dune': {
            'name': 'Dune',
            'url': 'https://dune.com'
        },
    }

# Metrics
## Level: either chain, da_layer, or app
## Fundamental: whether the metric should be considered a fundamentals metrics that is also exported in jsons
## Metric Keys: the keys that are used to query the database
## Units: the units that are used to display the metric
## Avg: whether the metric should be averaged over 7 days (rolling average)
## All L2s Aggregate: how the metric should be aggregated across all L2s (sum, avg, weighted_mean)
## Monthly Agg: how the metric should be aggregated monthly (sum, avg, maa)
## Max Date Fill: if max_date_fill is True, fill missing rows until yesterday with 0
## Ranking Bubble: whether the metric should be shown in the ranking bubble chart
## Ranking Landing: whether the metric should be shown in the ranking landing page
## Log Default: whether the metric should be shown by default on a log scale
## Hourly_available: whether the metric is available on an hourly basis (if True, an additional dict with hourly data will be exported

gtp_metrics_new = {
        ## Chain Level Metrics
        "chains": {
            'tvl': {
                'name': 'Total Value Secured',
                'icon': 'metrics-total-value-secured',
                'category': 'value-locked',
                'fundamental': True,
                'metric_keys': ['tvl', 'tvl_eth'],
                'units': {
                    'usd': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}, 
                    'eth': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
                },
                'avg': False, ##7d rolling average
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'ranking_bubble': False,
                'ranking_landing': True,
                'log_default': False,
                'url_path':  "/fundamentals/total-value-secured",
                'hourly_available': False
            }
            ,'txcount': {
                'name': 'Transaction Count',
                'icon': 'metrics-transaction-count',
                'category': 'activity',
                'fundamental': True,
                'metric_keys': ['txcount'],
                'units': {
                    'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': False,
                'ranking_landing': True,
                'log_default': False,
                'url_path': "/fundamentals/transaction-count",
                'hourly_available': True
            }
            ,'daa': {
                'name': 'Active Addresses',
                'icon': 'metrics-active-addresses',
                'category': 'activity',
                'fundamental': True,
                'metric_keys': ['daa'],
                'units': {
                    'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'maa',
                'max_date_fill' : False,
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False,
                'url_path': "/fundamentals/daily-active-addresses",
                'hourly_available': True
            }
            ,'stables_mcap': {
                'name': 'Stablecoin Supply',
                'icon': 'metrics-stablecoin-market-cap',
                'category': 'value-locked',
                'fundamental': True,
                'metric_keys': ['stables_mcap', 'stables_mcap_eth'],
                'units': {
                    'usd': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}, 
                    'eth': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
                },
                'avg': False,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False,
                'url_path': "/fundamentals/stablecoin-market-cap",
                'hourly_available': False
            }
            ,'fees': {
                'name': 'Revenue',
                'icon': 'metrics-revenue',
                'category': 'business',
                'fundamental': True,
                'metric_keys': ['fees_paid_usd', 'fees_paid_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False,
                'url_path': "/fundamentals/fees-paid-by-users",
                'hourly_available': True
            }
            ,'rent_paid': {
                'name': 'Rent Paid to L1',
                'icon': 'metrics-rent-paid-to-l1',
                'category': 'business',
                'fundamental': True,
                'metric_keys': ['rent_paid_usd', 'rent_paid_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : True,
                'ranking_bubble': False,
                'ranking_landing': True,
                'log_default': False,
                'url_path': "/fundamentals/rent-paid",
                'hourly_available': False
            }
            ,'profit': {
                'name': 'Onchain Profit',
                'icon': 'metrics-onchain-profit',
                'category': 'business',
                'fundamental': True,
                'metric_keys': ['profit_usd', 'profit_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : True,
                'ranking_bubble': False,
                'ranking_landing': True,
                'log_default': False,
                'url_path': "/fundamentals/profit",
                'hourly_available': False
            }
            ,'txcosts': {
                'name': 'Transaction Costs',
                'icon': 'metrics-transaction-cost',
                'category': 'convenience',
                'fundamental': True,
                'metric_keys': ['txcosts_median_usd', 'txcosts_median_eth'],
                'units': {
                    'usd': {'decimals': 4, 'decimals_tooltip': 4, 'agg_tooltip': False, 'agg': False}, 
                    'eth': {'decimals': 8, 'decimals_tooltip': 8, 'agg_tooltip': False, 'agg': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'weighted_mean',
                'monthly_agg': 'avg',
                'max_date_fill' : True,
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False,
                'url_path': "/fundamentals/transaction-costs",
                'hourly_available': True
            }
            ,'fdv': {
                'name': 'Fully Diluted Valuation',
                'icon': 'metrics-fully-diluted-valuation',
                'category': 'market',
                'fundamental': True,
                'metric_keys': ['fdv_usd', 'fdv_eth'],
                'units': {
                    'usd': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': True}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False,
                'url_path': "/fundamentals/fully-diluted-valuation",
                'hourly_available': False
            }
            ,'market_cap': {
                'name': 'Market Cap',
                'icon': 'metrics-market-cap',
                'category': 'market',
                'fundamental': True,
                'metric_keys': ['market_cap_usd', 'market_cap_eth'],
                'units': {
                    'usd': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': True}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'ranking_bubble': False,
                'ranking_landing': True,
                'log_default': False,
                'url_path': "/fundamentals/market-cap",
                'hourly_available': False
            }
            ,'throughput': {
                'name': 'Throughput',
                'icon': 'metrics-throughput',
                'category': 'activity',
                'fundamental': True,
                'metric_keys': ['gas_per_second'],
                'units': {
                    'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False, 'agg': False, 'suffix': 'Mgas/s'},
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False,
                'url_path': "/fundamentals/throughput",
                'hourly_available': True
            }         
            ,'app_revenue': {
                'name': 'App Revenue',
                'icon': 'metrics-revenue',
                'category': 'business',
                'fundamental': True,
                'metric_keys': ['app_fees_usd', 'app_fees_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': True,
                'ranking_landing': True,
                'log_default': False,
                'url_path': "/fundamentals/app-revenue",
                'hourly_available': False
            }

            ## Non Fundamental Metrics
            ,'costs': {
                'name': 'Costs',
                'fundamental': False, ## not a fundamental metric
                'metric_keys': ['costs_total_usd', 'costs_total_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}, 
                    'eth': {'decimals': 4, 'decimals_tooltip': 4, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': False,
                'ranking_landing': False,
                'log_default': False,
                'hourly_available': False
            }

            ,'costs_l1': {
                'name': 'L1 Costs',
                'fundamental': False, ## not a fundamental metric
                'metric_keys': ['costs_l1_usd', 'costs_l1_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}, 
                    'eth': {'decimals': 4, 'decimals_tooltip': 4, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': False,
                'ranking_landing': False,
                'log_default': False,
                'hourly_available': False
            }

            ,'costs_blobs': {
                'name': 'Blobs',
                'fundamental': False, ## not a fundamental metric
                'metric_keys': ['costs_blobs_usd', 'costs_blobs_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}, 
                    'eth': {'decimals': 4, 'decimals_tooltip': 4, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': False,
                'ranking_landing': False,
                'log_default': False,
                'hourly_available': False
            }
            
             ,'blob_size': {
                'name': 'Blob Size',
                'fundamental': False, ## not a fundamental metric
                'metric_keys': ['blob_size_bytes'],
                'units': {
                    'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True, 'suffix': 'GB'}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'ranking_bubble': False,
                'ranking_landing': False,
                'log_default': False,
                'hourly_available': False
            }
        },
        
        ## DA Layer Metrics
        'data_availability': {
            'blob_count': {
                'name': 'Blob Count',
                'icon': 'da-blobs-number',
                'category': 'metrics',
                'fundamental': True,
                'metric_keys': ['da_blob_count'],
                'units': {
                    'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': True}
                },
                'avg': True, ##7d rolling average
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'log_default': False,
                'hourly_available': False
            }
            ,'data_posted': {
                'name': 'Data Posted',
                'icon': 'da-data-posted',
                'category': 'metrics',
                'fundamental': True,
                'metric_keys': ['da_data_posted_bytes'],
                'units': {
                    'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True, 'suffix': 'GB'}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'log_default': False,
                'hourly_available': False
            }
            ,'fees_paid': {
                'name': 'DA Fees Paid',
                'icon': 'da-fees-paid',
                'category': 'metrics',
                'fundamental': True,
                'metric_keys': ['da_fees_usd', 'da_fees_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}, 
                    'eth': {'decimals': 4, 'decimals_tooltip': 4, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'log_default': True,
                'hourly_available': False
            }
            ,'fees_per_mbyte': {
                'name': 'Fees Paid per MB',
                'icon': 'da-fees-paid-per-mb',
                'category': 'metrics',
                'fundamental': True,
                'metric_keys': ['da_fees_per_mbyte_usd', 'da_fees_per_mbyte_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                    'eth': {'decimals': 6, 'decimals_tooltip': 6, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'avg',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'log_default': True,
                'hourly_available': False
            }
            ,'blob_producers': {
                'name': 'DA Consumers',
                'icon': 'da-consumers',
                'category': 'metrics',
                'fundamental': True,
                'metric_keys': ['da_unique_blob_producers'],
                'units': {
                    'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'log_default': False,
                'hourly_available': False
            }
        },
        
        ## App Level Metrics
        'apps': {
            'txcount': {
                'name': 'Transaction Count',
                'icon': 'metrics-transaction-count',
                'category': 'activity',
                'fundamental': True,
                'description': 'Number of transactions sent to contracts that are mapped to the application.',
                'metric_keys': ['txcount'],
                'units': {
                    'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'source': ['RPC', 'OLI'],
                'hourly_available': False,
                'chain_specific': True,
                'toggles': ['absolute', 'stacked', 'percentage']
            }
            ,'daa': {
                'name': 'Active Addresses',
                'icon': 'metrics-activeaddresses',
                'category': 'activity',
                'fundamental': True,
                'description': 'Number of unique addresses that have interacted with the application.',
                'metric_keys': ['daa'],
                'units': {
                    'value': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'maa',
                'max_date_fill' : False,
                'source': ['RPC', 'OLI'],
                'hourly_available': False,
                'chain_specific': True,
                'toggles': ['absolute', 'stacked', 'percentage']
            }
            ,'gas_fees': {
                'name': 'Gas Fees Paid',
                'icon': 'metrics-revenue',
                'category': 'activity',
                'fundamental': True,
                'description': 'Gas fees (paid by users) towards the underlying chains in order to use the application.',
                'metric_keys': ['fees_paid_usd', 'fees_paid_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'sum',
                'max_date_fill' : False,
                'source': ['RPC', 'OLI'],
                'hourly_available': False,
                'chain_specific': True,
                'toggles': ['absolute', 'stacked', 'percentage']
            }
            ,'success_rate': {
                'name': 'Success Rate',
                'icon': 'rank',
                'category': 'activity',
                'fundamental': False,
                'description': 'Percentage of transactions to the application that were successful.',
                'metric_keys': ['success_rate'],
                'units': {
                    'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
                },
                'avg': True,
                'all_l2s_aggregate': 'avg',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'source': ['RPC', 'OLI'],
                'hourly_available': False,
                'chain_specific': True,
                'toggles': ['absolute']
            }
            ,'market_cap': {
                'name': 'Market Cap',
                'icon': 'metrics-marketcap',
                'category': 'market',
                'fundamental': False,
                'description': 'Market capitalization of the application token.',
                'metric_keys': ['market_cap_usd', 'market_cap_eth'],
                'units': {
                    'usd': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': True}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'source': ['coingecko'],
                'hourly_available': False,
                'chain_specific': False,
                'toggles': ['absolute', 'stacked', 'percentage']
            }
            ,'token_volume': {
                'name': 'Token Trading Volume',
                'icon': 'tokentransfers',
                'category': 'market',
                'fundamental': False,
                'description': 'Trading volume of the application token.',
                'metric_keys': ['volume_usd', 'volume_eth'],
                'units': {
                    'usd': {'decimals': 0, 'decimals_tooltip': 0, 'agg_tooltip': True}, 
                    'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'source': ['coingecko'],
                'hourly_available': False,
                'chain_specific': False,
                'toggles': ['absolute', 'stacked', 'percentage']
            }
            ,'token_price': {
                'name': 'Token Price',
                'icon': 'monitor',
                'category': 'market',
                'fundamental': False,
                'description': 'Price of the application token.',
                'metric_keys': ['price_usd', 'price_eth'],
                'units': {
                    'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}, 
                    'eth': {'decimals': 8, 'decimals_tooltip': 8, 'agg_tooltip': True}
                },
                'avg': True,
                'all_l2s_aggregate': 'sum',
                'monthly_agg': 'avg',
                'max_date_fill' : False,
                'source': ['coingecko'],
                'hourly_available': False,
                'chain_specific': False,
                'toggles': ['absolute']
            }
        }
        
        # Eth is Money metrics (on Ethereum Level)
        ,'eim': {
            'eth_exported': {
            'name': 'ETH exported',
            'fundamental': True,
            'metric_keys': ['eth_equivalent_exported_usd', 'eth_equivalent_exported_eth'],
            'units': {
                'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
            },
            'avg': False, ##7d rolling average
            'monthly_agg': 'sum',
            'max_date_fill' : False,
            'log_default': False
        },
        'eth_supply': {
            'name': 'ETH supply',
            'fundamental': True,
            'metric_keys': ['eth_supply_eth'],
            'units': {
                'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
            },
            'avg': False, ##7d rolling average
            'monthly_agg': 'avg',
            'max_date_fill' : False,
            'log_default': False
        },
        'eth_issuance_rate': {
            'name': 'ETH issuance rate',
            'fundamental': True,
            'metric_keys': ['eth_issuance_rate'],
            'units': {
                'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
            },
            'avg': False, ##7d rolling average
            'monthly_agg': 'avg',
            'max_date_fill' : False,
            'log_default': False
        }
    }
}

gtp_fees_types = {
        'txcosts_median' : {
            'name': 'Median Fee',
            'name_short': 'Median Fee',
            'metric_keys': ['txcosts_median_eth'],
            'units': {
                'usd': {'decimals': 3, 'decimals_tooltip': 3, 'agg_tooltip': False, 'agg': False}, 
                'eth': {'decimals': 8, 'decimals_tooltip': 8, 'agg_tooltip': False, 'agg': False}
            },
            'category': 'Fees',
            'currency': True,
            'priority': 1,
            'invert_normalization': False
        }
        ,'txcosts_native_median' : {
            'name': 'Transfer ETH Fee',
            'name_short': 'Transfer ETH',
            'metric_keys': ['txcosts_native_median_eth'],
            'units': {
                'usd': {'decimals': 3, 'decimals_tooltip': 3, 'agg_tooltip': False, 'agg': False}, 
                'eth': {'decimals': 8, 'decimals_tooltip': 8, 'agg_tooltip': False, 'agg': False}
            },
            'category': 'Fees',
            'currency': True,
            'priority': 2,
            'invert_normalization': False
        }
        , 'tps' : {
            'name': 'Transactions per Second',
            'name_short': 'TPS',
            'metric_keys': ['txcount'],
            'units': {
                'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False, 'agg': False},
            },
            'category': 'Activity',
            'currency': False,
            'priority': 3,
            'invert_normalization': True
        }
        , 'throughput' : {
            'name': 'Throughput',
            'name_short': 'Throughput',
            'metric_keys': ['gas_per_second'],
            'units': {
                'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False, 'agg': False, 'suffix': 'Mgas/s'},
            },
            'category': 'Activity',
            'currency': False,
            'priority': 4,
            'invert_normalization': True
        }
        , 'txcosts_swap' : {
            'name': 'Swap Fee',
            'name_short': 'Swap Fee',
            'metric_keys': ['txcosts_swap_eth'],
            'units': {
                'usd': {'decimals': 3, 'decimals_tooltip': 3, 'agg_tooltip': False, 'agg': False}, 
                'eth': {'decimals': 8, 'decimals_tooltip': 8, 'agg_tooltip': False, 'agg': False}
            },
            'category': 'Fees',
            'currency': True,
            'priority': 5,
            'invert_normalization': False
        }
        ,'txcosts_avg' : {
            'name': 'Average Fee',
            'name_short': 'Average Fee',
            'metric_keys': ['txcosts_avg_eth'],
            'units': {
                'usd': {'decimals': 3, 'decimals_tooltip': 3, 'agg_tooltip': False, 'agg': False}, 
                'eth': {'decimals': 8, 'decimals_tooltip': 8, 'agg_tooltip': False, 'agg': False}
            },
            'category': 'Fees',
            'currency': True,
            'priority': 6,
            'invert_normalization': False
        }          
    }

# Fees timespans
## mapping of timeframes to granularity, filter_days and tps_divisor
gtp_fees_timespans = {
        '24hrs' : {'granularity': '10_min', 'filter_days': 1, 'tps_divisor': 60*10},
        '7d' : {'granularity': 'hourly', 'filter_days': 7, 'tps_divisor': 60*60},
        '30d' : {'granularity': '4_hours', 'filter_days': 30, 'tps_divisor': 60*60*4},
        '180d' : {'granularity': 'daily', 'filter_days': 180, 'tps_divisor': 60*60*24},
    }

l2_maturity_levels = {
        "10_foundational": {
            "name": "Foundational",
            "description": "Ethereum Mainnet, a fully decentralized and secure network that anchors the entire ecosystem.",
            "conditions": "Be Ethereum"
        },
        "4_robust": {
            "name" : "Robust",
            "description" : "Fully decentralized and secure network that cannot be tampered with or stopped by any individual or group, including its creators. This is a network that fulfills Ethereum's vision of decentralization.",
            "conditions": {
                "and" : {
                    "tvs": 1000000000,
                    "stage": "Stage 2",
                    "age": 0,
                    "risks": 0
                }
            }
        },
        "3_maturing": {
            "name" : "Maturing",
            "description" : "A network transitioning to being decentralized. A group of actors still may be able to halt the network in extreme situations.",
            "conditions": {
                "and" : {
                    "tvs": 150000000,
                    "stage": "Stage 1",
                    "age": 180,
                    "risks": 0
                }
            }
        },
        "2_developing": {
            "name" : "Developing",
            "description" : "A centralized operator runs the network but adds fail-safe features to reduce risks of centralization.",
            "conditions": {
                "and" : {
                    "tvs": 150000000,
                    "stage": "Stage 0",
                    "risks": 3,
                    "age": 180 
                }
            }
        },
        "1_emerging": {
            "name" : "Emerging",
            "description" : "A centralized operator runs the network. The data is publicly visible on Ethereum to verify whether the operator is being honest.",
            "conditions": {
                "and" : {
                    "stage": "Stage 0",
                    "risks": 2
                },
                "or" : {
                    "tvs": 150000000,
                    "age": 180 
                }
            }
        },
        "0_early_phase": {
            "name" : "Not Categorized",
            "description" : "A project that just recently launched or that doesn't currently fall into any of the other categories.",
        }
    }

main_chart_config = {
    "defs": {
        "gradients": [
        {
            "id": "cross_layer_background_gradient",
            "config": {
            "type": "linearGradient",
            "linearGradient": { "x1": 1.5, "y1": 1.5, "x2": -1, "y2": -1 },
            "stops": [[0, "#FE5468"], [1, "#FFDF27"]]
            }
        }
        ],
        "patterns": []
    },
    "composition_types": {
        "main_l1": {
            "order": 0,
            "name": "Ethereum Mainnet",
            "description": "Ethereum Mainnet data",
            "fill": {
                "type": "gradient",
                "config": {
                "type": "linearGradient",
                "linearGradient": { "x1": 0, "y1": 0, "x2": 0, "y2": 1 },
                "stops": [[0, "#94ABD3"], [1, "#596780"]]
                }
            }
        },
        "main_l2": {
            "order": 1,
            "name": "Layer 2",
            "description": "Layer 2 scaling solutions",
            "fill": {
                "type": "gradient",
                "config": {
                "type": "linearGradient",
                "linearGradient": { "x1": 0, "y1": 1, "x2": 0, "y2": 0 },
                "stops": [[0, "#FE5468"], [1, "#FFDF27"]]
                }
            }
        },
        "only_l1": {
            "order": 0,
            "name": "Ethereum Mainnet",
            "description": "Only users that interacted with Ethereum Mainnet but not with any L2.",
            "fill": {
                "type": "gradient",
                "config": {
                "type": "linearGradient",
                "linearGradient": { "x1": 0, "y1": 0, "x2": 0, "y2": 1 },
                "stops": [[0, "#94ABD3"], [1, "#596780"]]
                }
            }
        },
        "cross_layer": {
            "order": 1,
            "name": "Cross-Layer",
            "description": "Users that interacted with Ethereum Mainnet and at least one L2.",
            "fill": {
                "type": "pattern",
                "config": {
                "type": "colored-hash",
                "direction": "right",
                "color": "#94ABD3",
                "backgroundFill": "url(#cross_layer_background_gradient)"
                }
            }
        },
        "multiple_l2s": {
            "order": 2,
            "name": "Multiple Layer 2s",
            "description": "Users that interacted with multiple L2s but not Ethereum Mainnet.",
            "fill": {
                "type": "gradient",
                "config": {
                "type": "linearGradient",
                "linearGradient": { "x1": 0, "y1": 0, "x2": 1, "y2": 1 },
                "stops": [[0, "#FE5468"], [1, "#FFDF27"]]
                }
            }
            },
        "single_l2": {
            "order": 3,
            "name": "Single Layer 2",
            "description": "Users that interacted with a single L2 but not Ethereum Mainnet.",
            "fill": {
                "type": "gradient",
                "config": {
                "type": "linearGradient",
                "linearGradient": { "x1": 0, "y1": 0, "x2": 1, "y2": 1 },
                "stops": [[0, "#FE5468"], [1, "#FFDF27"]]
                }
            },
            "mask": {
                "config": {
                "direction": "right"
                }
            }
            },
        "all_l2s": {
            "order": 4,
            "name": "All Layer 2s",
            "description": "Users that interacted with all L2s.",
            "fill": {
                "type": "gradient",
                "config": {
                "type": "linearGradient",
                "linearGradient": { "x1": 0, "y1": 1, "x2": 0, "y2": 0 },
                "stops": [[0, "#FE5468"], [1, "#FFDF27"]]
                }
            }
        }
    }
}


# Achievements Levels for chains based on different metrics
levels_dict = {
    'txcount': {
        0: 0,
        1: 50_000,
        2: 100_000,
        3: 250_000,
        4: 500_000,
        5: 1_000_000, # 1 million
        6: 2_500_000,
        7: 5_000_000,
        8: 10_000_000,
        9: 25_000_000,
        10: 50_000_000,
        11: 100_000_000,
        12: 250_000_000,
        13: 500_000_000,
        14: 1_000_000_000, # 1 billion
        15: 2_500_000_000,
        16: 5_000_000_000,
        17: 10_000_000_000,
        18: 25_000_000_000,
        19: 50_000_000_000,
        20: 100_000_000_000,
        21: 150_000_000_000,
        22: 200_000_000_000,
        23: 300_000_000_000,
        24: 400_000_000_000,
        25: 500_000_000_000,
        26: 600_000_000_000,
        27: 700_000_000_000,
        28: 800_000_000_000,
        29: 900_000_000_000,
        30: 1_000_000_000_000,  # 1 trillion
    },
    "daa": {
        0: 0,
        1: 1_000,
        2: 2_500,
        3: 5_000,
        4: 10_000,
        5: 25_000,
        6: 50_000,
        7: 100_000,
        8: 250_000,
        9: 500_000,
        10: 1_000_000, # 1 million
        11: 2_500_000,
        12: 5_000_000,
        13: 10_000_000,
        14: 25_000_000,
        15: 50_000_000,
        16: 100_000_000,
        17: 250_000_000,
        18: 500_000_000,
        19: 1_000_000_000, # 1 billion
        20: 2_000_000_000,
        21: 3_000_000_000,
        22: 5_000_000_000,
        23: 7_500_000_000,
        24: 10_000_000_000,
        25: 12_500_000_000,
        26: 15_000_000_000,
        27: 20_000_000_000,
        28: 30_000_000_000,
        29: 40_000_000_000,
        30: 50_000_000_000, # 50 billion
    },
    "fees_paid_usd": {
        0: 0,
        1: 10_000,
        2: 25_000,
        3: 50_000,
        4: 100_000,
        5: 250_000,
        6: 500_000,
        7: 1_000_000, # 1 million
        8: 2_500_000,
        9: 5_000_000,
        10: 10_000_000,
        11: 25_000_000,
        12: 50_000_000,
        13: 100_000_000,
        14: 250_000_000,
        15: 500_000_000,
        16: 1_000_000_000, # 1 billion
        17: 2_500_000_000,
        18: 5_000_000_000,
        19: 10_000_000_000,
        20: 25_000_000_000,
        21: 50_000_000_000,
        22: 100_000_000_000,
        23: 250_000_000_000,
        24: 500_000_000_000,
        25: 1_000_000_000_000, # 1 trillion
        26: 2_500_000_000_000,
        27: 5_000_000_000_000,
        28: 10_000_000_000_000,
        29: 25_000_000_000_000,
        30: 50_000_000_000_000, # 50 trillion
    },
    "fees_paid_eth": {
        0: 0,
        1: 1,
        2: 5,
        3: 10,
        4: 25,
        5: 50,
        6: 100,
        7: 250,
        8: 500,
        9: 1_000, # 1 thousand
        10: 2_500,
        11: 5_000,
        12: 10_000,
        13: 25_000,
        14: 50_000,
        15: 100_000,
        16: 250_000,
        17: 500_000,
        18: 1_000_000, # 1 million
        19: 2_000_000,
        20: 3_000_000,
        21: 5_000_000,
        22: 7_500_000,
        23: 10_000_000,
        24: 25_000_000,
        25: 50_000_000,
        26: 100_000_000,
        27: 250_000_000, 
        28: 500_000_000,
        29: 750_000_000, 
        30: 1_000_000_000, # 1 billion
    },
    "profit_usd": {
        0: 0,
        1: 10_000,
        2: 25_000,
        3: 50_000,
        4: 100_000,
        5: 250_000,
        6: 500_000,
        7: 1_000_000, # 1 million
        8: 2_500_000,
        9: 5_000_000,
        10: 10_000_000,
        11: 25_000_000,
        12: 50_000_000,
        13: 100_000_000,
        14: 250_000_000,
        15: 500_000_000,
        16: 1_000_000_000, # 1 billion
        17: 2_500_000_000,
        18: 5_000_000_000,
        19: 10_000_000_000,
        20: 25_000_000_000,
        21: 50_000_000_000,
        22: 100_000_000_000,
        23: 250_000_000_000,
        24: 500_000_000_000,
        25: 1_000_000_000_000, # 1 trillion
        26: 2_500_000_000_000,
        27: 5_000_000_000_000,
        28: 10_000_000_000_000,
        29: 25_000_000_000_000,
        30: 50_000_000_000_000, # 50 trillion
    },
    "profit_eth": {
        0: 0,
        1: 1,
        2: 5,
        3: 10,
        4: 25,
        5: 50,
        6: 100,
        7: 250,
        8: 500,
        9: 1_000, # 1 thousand
        10: 2_500,
        11: 5_000,
        12: 10_000,
        13: 25_000,
        14: 50_000,
        15: 100_000,
        16: 250_000,
        17: 500_000,
        18: 1_000_000, # 1 million
        19: 2_000_000,
        20: 3_000_000,
        21: 5_000_000,
        22: 7_500_000,
        23: 10_000_000,
        24: 25_000_000,
        25: 50_000_000,
        26: 100_000_000,
        27: 250_000_000, 
        28: 500_000_000,
        29: 750_000_000, 
        30: 1_000_000_000, # 1 billion
    },
    "rent_paid_usd": {
        0: 0,
        1: 1_000,
        2: 2_500,
        3: 5_000,
        4: 10_000,
        5: 25_000,
        6: 50_000,
        7: 100_000,
        8: 250_000,
        9: 500_000,
        10: 1_000_000, # 1 million
        11: 2_500_000,
        12: 5_000_000,
        13: 10_000_000,
        14: 25_000_000,
        15: 50_000_000,
        16: 100_000_000,
        17: 250_000_000,
        18: 500_000_000,
        19: 1_000_000_000, # 1 billion
        20: 2_000_000_000,
        21: 3_000_000_000,
        22: 5_000_000_000,
        23: 7_500_000_000,
        24: 10_000_000_000,
        25: 12_500_000_000,
        26: 15_000_000_000,
        27: 20_000_000_000,
        28: 30_000_000_000,
        29: 40_000_000_000,
        30: 50_000_000_000, # 50 billion
    },
    "rent_paid_eth": {
        0: 0,
        1: 0.25,
        2: 0.5,
        3: 1,
        4: 2.5,
        5: 5,
        6: 10,
        7: 25,
        8: 50,
        9: 100,
        10: 250,
        11: 500,
        12: 1_000, # 1 thousand
        13: 2_500,
        14: 5_000,
        15: 10_000,
        16: 25_000,
        17: 50_000,
        18: 100_000,
        19: 250_000,
        20: 500_000,
        21: 1_000_000, # 1 million
        22: 2_000_000,
        23: 3_000_000,
        24: 5_000_000,
        25: 7_500_000,
        26: 10_000_000,
        27: 12_500_000,
        28: 15_000_000,
        29: 17_500_000,
        30: 20_000_000, # 20 million
    },
}

# Highlight thresholds
# txcount, daa
activity_list = [
        1_000, 2_000, 3_000, 4_000, 5_000, 6_000, 7_000, 8_000, 9_000,
        10_000, 20_000, 25_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000,
        100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 700_000, 800_000, 900_000,
        1_000_000, 1_500_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000,
                6_000_000, 7_000_000, 8_000_000, 9_000_000, 
        10_000_000, 11_000_000, 12_000_000, 13_000_000, 14_000_000, 15_000_000, 17_500_000, 
                20_000_000, 30_000_000, 40_000_000, 50_000_000, 60_000_000, 70_000_000, 75_000_000, 80_000_000, 90_000_000,
        100_000_000, 150_000_000, 200_000_000, 300_000_000, 400_000_000, 500_000_000, 600_000_000, 700_000_000, 800_000_000, 900_000_000,
        1_000_000_000
        ]

# throughput (gas per second)
throughput_list = [
        100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 700_000, 800_000, 900_000,
        1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000, 7_000_000, 8_000_000, 9_000_000,
        10_000_000, 15_000_000, 20_000_000, 25_000_000, 30_000_000, 40_000_000, 50_000_000, 60_000_000, 70_000_000, 80_000_000, 90_000_000,
        100_000_000, 200_000_000, 300_000_000, 400_000_000, 500_000_000, 600_000_000, 700_000_000, 800_000_000, 900_000_000,
        1_000_000_000, 2_000_000_000, 3_000_000_000, 4_000_000_000, 5_000_000_000, 6_000_000_000, 7_000_000_000, 8_000_000_000, 9_000_000_000,
        10_000_000_000
        ]

# fees, profit
fees_list_usd = [
        1_000, 2_000, 3_000, 4_000, 5_000, 6_000, 7_000, 8_000, 9_000,
        10_000, 20_000, 25_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000,
        100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 700_000, 800_000, 900_000,
        1_000_000, 1_500_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000,
                6_000_000, 7_000_000, 8_000_000, 9_000_000,
        10_000_000, 11_000_000, 12_500_000, 15_000_000, 20_000_000, 30_000_000, 40_000_000, 50_000_000, 
                60_000_000, 70_000_000, 80_000_000, 90_000_000,
        100_000_000, 200_000_000, 300_000_000, 400_000_000, 500_000_000, 600_000_000, 700_000_000, 800_000_000, 900_000_000,
        1_000_000_000
        ]

fees_list_eth = [
        1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90,
        100, 150, 200, 300, 400, 500, 600, 700, 800, 900, 
        1_000, 1_500, 2_000, 3_000, 4_000, 5_000, 6_000, 7_000, 8_000, 9_000,
        10_000, 12_500, 15_000, 20_000, 30_000, 40_000, 50_000, 60_000, 70_000, 80_000, 90_000,
        100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 700_000, 800_000, 900_000,
        ],

# stables, tvl, mcap, fdv
value_locked_list_usd = [
        100_000, 200_000, 300_000, 400_000, 500_000, 600_000, 700_000, 800_000, 900_000,
        1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000, 6_000_000, 7_000_000, 8_000_000, 9_000_000,
        10_000_000, 12_500_000, 15_000_000, 20_000_000, 30_000_000, 40_000_000, 50_000_000, 60_000_000, 70_000_000, 80_000_000, 90_000_000,
        100_000_000, 200_000_000, 300_000_000, 400_000_000, 500_000_000, 600_000_000, 700_000_000, 800_000_000, 900_000_000, 
        1_000_000_000, 2_000_000_000, 3_000_000_000, 4_000_000_000, 5_000_000_000, 6_000_000_000, 7_000_000_000, 8_000_000_000, 9_000_000_000, 
        10_000_000_000, 15_000_000_000, 20_000_000_000, 25_000_000_000,
                30_000_000_000, 40_000_000_000, 50_000_000_000, 60_000_000_000, 70_000_000_000, 80_000_000_000, 90_000_000_000, 
        100_000_000_000, 200_000_000_000, 300_000_000_000, 400_000_000_000, 500_000_000_000, 
                600_000_000_000, 700_000_000_000, 800_000_000_000, 900_000_000_000,
        1_000_000_000_000, 2_000_000_000_000, 3_000_000_000_000, 4_000_000_000_000, 5_000_000_000_000, 
                6_000_000_000_000, 7_000_000_000_000, 8_000_000_000_000, 9_000_000_000_000,
        10_000_000_000_000
        ],

relative_growth_dict_activity = {
        'agg': 'sum',
        'thresholds': {
                1: 0.50,
                7: 0.75
        }
}

relative_growth_dict_activity_higher = {
        'agg': 'sum',
        'thresholds': {
                1: 2,
                7: 3
        }
}


relative_growth_dict_value_locked = {
        'agg': 'last',
        'thresholds': {
                1: 0.20,
                7: 0.30
        }
}

highlights_daily_thresholds = {
        'txcount': {
                'ath_multiples' : activity_list,
                'relative_growth' : relative_growth_dict_activity
        },
        'daa': {
                'ath_multiples' : activity_list,
                'relative_growth' : relative_growth_dict_activity
        },
        'fees_paid_usd': {
                'ath_multiples' : fees_list_usd,
                'relative_growth' : relative_growth_dict_activity_higher
        },
        'fees_paid_eth': {
                'ath_multiples' : fees_list_eth
        },
        'profit_usd': {
                'ath_multiples' : fees_list_usd,
                'relative_growth' : relative_growth_dict_activity_higher
        },
        'profit_eth': {
                'ath_multiples' : fees_list_eth
        },
        'app_fees_usd': {
                'ath_multiples' : fees_list_usd,
                'relative_growth' : relative_growth_dict_activity_higher
        },
        'app_fees_eth': {
                'ath_multiples' : fees_list_eth
        },
        'stables_mcap': {
                'ath_multiples' : value_locked_list_usd,
                'relative_growth' : relative_growth_dict_value_locked
        },
        'tvl': {
                'ath_multiples' : value_locked_list_usd,
                'relative_growth' : relative_growth_dict_value_locked
        },
        'gas_per_second': {
                'ath_multiples' : throughput_list,
        },
        'market_cap_usd': {
                'ath_multiples' : value_locked_list_usd,
                'relative_growth' : relative_growth_dict_value_locked
        },
        'fdv_usd': {
                'ath_multiples' : value_locked_list_usd,
                'relative_growth' : relative_growth_dict_value_locked
        }
}

eim_metrics = {
        'eth_exported': {
            'name': 'ETH exported',
            'fundamental': True,
            'metric_keys': ['eth_equivalent_exported_usd', 'eth_equivalent_exported_eth'],
            'units': {
                'usd': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}, 
                'eth': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': False}
            },
            'avg': False, ##7d rolling average
            'monthly_agg': 'sum',
            'max_date_fill' : False,
            'log_default': False
        },
        'eth_supply': {
            'name': 'ETH supply',
            'fundamental': True,
            'metric_keys': ['eth_supply_eth'],
            'units': {
                'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
            },
            'avg': False, ##7d rolling average
            'monthly_agg': 'avg',
            'max_date_fill' : False,
            'log_default': False
        },
        'eth_issuance_rate': {
            'name': 'ETH issuance rate',
            'fundamental': True,
            'metric_keys': ['eth_issuance_rate'],
            'units': {
                'value': {'decimals': 2, 'decimals_tooltip': 2, 'agg_tooltip': True}
            },
            'avg': False, ##7d rolling average
            'monthly_agg': 'avg',
            'max_date_fill' : False,
            'log_default': False
        }
    }
