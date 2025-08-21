
# Structure:
# - Each entry corresponds to a chain in ther Ethereum ecosystem (Mainnet and Layer 2s).
# - Each entry contains:
#   - `name`: The name of the chain (only used for display purposes if chain not yet listed on growthepie).
#   - `processors`: The type of processor to use (e.g., "evm", "starknet") which determines how the chain's data is processed.
#   - `stack`: The stack type (e.g., "l1", "op_stack", "nitro", "elastic", "basic") which determines the fee calculation and other processing specifics.
#   - `sleeper`: The time to wait between requests in seconds.
#   - `calc_fees`: A boolean indicating whether to calculate fees for transactions on this chain.

rpc_config = {
    ## L1
    "ethereum": {
        "name": "Ethereum",
        "processors": "evm",
        "stack": "l1",
        "sleeper": 6,
        "calc_fees": True,
    },
    
    ## OP Stack chains
    "mode": {
        "name": "Mode",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 5,
        "calc_fees": True,
    },
    "base": {
        "name": "Base",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 2,
        "calc_fees": True,
    },
    "optimism": {
        "name": "OP Mainnet",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "blast": {
        "name": "Blast",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 5,
        "calc_fees": True,
    },
    "unichain": {
        "name": "Unichain",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "redstone": {
        "name": "Redstone",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 5,
        "calc_fees": True,
    },
    "soneium": {
        "name": "Soneium",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 10, # Longer sleeper due to limited RPC access via Startale
        "calc_fees": True,
    },
    "worldchain": {
        "name": "Worldchain",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "ink": {
        "name": "Ink",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "zora": {
        "name": "Zora",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 5,
        "calc_fees": True,
    },
    "swell": {
        "name": "Swell",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "towns": {
        "name": "Towns",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "lisk": {
        "name": "Lisk",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "katana": {
        "name": "Katana",
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": False,
    },
    
    ## Nitro/Arbitrum Orbit chains
    "arbitrum": {
        "name": "Arbitrum One",
        "processors": "evm",
        "stack": "nitro",
        "sleeper": 2,
        "calc_fees": True,
    },
    
    "arbitrum_nova": {
        "name": "Arbitrum Nova",
        "processors": "evm",
        "stack": "nitro",
        "sleeper": 3,
        "calc_fees": True,
    },
    
    "plume": {
        "name": "Plume",
        "processors": "evm",
        "stack": "nitro",
        "sleeper": 3,
        "calc_fees": False, ##custom gas?
    },
    
    # ZK Stack chains
    "zksync_era": {
        "name": "ZKsync Era",
        "processors": "evm",
        "stack": "zk_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "abstract": {
        "name": "Abstract",
        "processors": "evm",
        "stack": "zk_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    #ZK StacK, Custom Gas
    "sophon": {
        "name": "Sophon",
        "processors": "evm",
        "stack": "zk_stack",
        "sleeper": 3,
        "calc_fees": False,
    },
    
    ## Others
    "linea": {
        "name": "Linea",
        "processors": "evm",
        "stack": "basic",
        "sleeper": 5,
        "calc_fees": True,
    },
    
    "scroll": {
        "name": "Scroll",
        "processors": "evm",
        "stack": "basic",
        "sleeper": 5,
        "calc_fees": True,
    },
    
    "taiko": {
        "name": "Taiko Alethia",
        "processors": "evm",
        "stack": "basic",
        "sleeper": 6,
        "calc_fees": True,
    },
    
    "manta": {
        "name": "Manta Pacific",
        "processors": "evm",
        "sleeper": 3,
        "calc_fees": False,
    },
    
    "zircuit": {
        "name": "Zircuit",
        "processors": "evm",
        "sleeper": 3,
        "calc_fees": False,
    },

    # Custom Gas EVM chains
    "mantle": {
        "name": "Mantle",
        "processors": "evm_custom_gas",
        "sleeper": 3,
        "calc_fees": False,
    },
    "celo": {
        "name": "Celo",
        "processors": "evm_custom_gas",
        "sleeper": 2,
        "calc_fees": False,
    },

    # Non-EVM chains
    "starknet": {
        "name": "Starknet",
        "processors": "starknet",
        "sleeper": 5,
        "calc_fees": False,
    },
}