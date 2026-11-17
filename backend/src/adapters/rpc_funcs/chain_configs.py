chain_configs ={
  "default": {
    "required_columns": [
      "l1GasUsed",
      "l1GasPrice",
      "l1FeeScalar"
    ],
    "fillna_values": {
      "l1FeeScalar": "0"
    },
    "column_mapping": {
      "blockNumber": "block_number",
      "hash": "tx_hash",
      "from": "from_address",
      "to": "to_address",
      "gasPrice": "gas_price",
      "gas": "gas_limit",
      "gasUsed": "gas_used",
      "value": "value",
      "status": "status",
      "input": "empty_input",
      "l1GasUsed": "l1_gas_used",
      "l1GasPrice": "l1_gas_price",
      "l1FeeScalar": "l1_fee_scalar",
      "block_timestamp": "block_timestamp",
      "type": "tx_type",
      "nonce": "nonce"
    },
    "numeric_columns": [
      "gas_price",
      "gas_used",
      "l1_fee_scalar"
    ],
    "special_operations": [
      "handle_l1_gas_price",
      "handle_l1_fee_scalar",
      "handle_l1_gas_used",
      "calculate_tx_fee",
      "convert_input_to_boolean"
    ],
    "date_columns": {
      "block_timestamp": "s"
    },
    "status_mapping": {
      "0": 0,
      "1": 1,
      "default": -1
    },
    "bytea_columns": [
      "tx_hash",
      "to_address",
      "from_address"
    ],
    "value_conversion": {
      "gas_price": 1000000000000000000,
      "value": 1000000000000000000,
      "l1_gas_price": 1000000000000000000
    },
    "address_columns": [
      "to_address"
    ]
  },
  "linea": {
    "column_mapping": {
      "blockNumber": "block_number",
      "hash": "tx_hash",
      "from": "from_address",
      "to": "to_address",
      "gasPrice": "gas_price",
      "gas": "gas_limit",
      "gasUsed": "gas_used",
      "value": "value",
      "status": "status",
      "input": "empty_input",
      "block_timestamp": "block_timestamp",
      "type": "tx_type",
      "nonce": "nonce"
    },
    "numeric_columns": [
      "gas_price",
      "gas_used"
    ],
    "special_operations": [
      "calculate_tx_fee",
      "convert_input_to_boolean"
    ],
    "date_columns": {
      "block_timestamp": "s"
    },
    "status_mapping": {
      "0": 0,
      "1": 1,
      "default": -1
    },
    "bytea_columns": [
      "tx_hash",
      "to_address",
      "from_address"
    ],
    "value_conversion": {
      "gas_price": 1000000000000000000,
      "value": 1000000000000000000
    },
    "address_columns": [
      "to_address"
    ]
  },
  "scroll": {
    "column_mapping": {
      "blockNumber": "block_number",
      "hash": "tx_hash",
      "from": "from_address",
      "to": "to_address",
      "gasPrice": "gas_price",
      "gas": "gas_limit",
      "gasUsed": "gas_used",
      "value": "value",
      "status": "status",
      "input": "empty_input",
      "l1Fee": "l1_fee",
      "block_timestamp": "block_timestamp",
      "type": "tx_type",
      "nonce": "nonce"
    },
    "numeric_columns": [
      "gas_price",
      "gas_used"
    ],
    "special_operations": [
      "handle_l1_fee_scroll",
      "convert_input_to_boolean"
    ],
    "date_columns": {
      "block_timestamp": "s"
    },
    "status_mapping": {
      "0": 0,
      "1": 1,
      "default": -1
    },
    "bytea_columns": [
      "tx_hash",
      "to_address",
      "from_address"
    ],
    "value_conversion": {
      "gas_price": 1000000000000000000,
      "value": 1000000000000000000
    },
    "address_columns": [
      "to_address"
    ]
  },
  "arbitrum_nitro": {
    "column_mapping": {
      "blockNumber": "block_number",
      "hash": "tx_hash",
      "from": "from_address",
      "to": "to_address",
      "effectiveGasPrice": "gas_price",
      "gas": "gas_limit",
      "gasUsed": "gas_used",
      "value": "value",
      "status": "status",
      "input": "empty_input",
      "block_timestamp": "block_timestamp",
      "gasUsedForL1": "l1_gas_used",
      "type": "tx_type",
      "nonce": "nonce"
    },
    "numeric_columns": [
      "gas_price",
      "gas_used"
    ],
    "special_operations": [
      "handle_l1_gas_used",
      "calculate_tx_fee",
      "convert_input_to_boolean"
    ],
    "date_columns": {
      "block_timestamp": "s"
    },
    "status_mapping": {
      "0": 0,
      "1": 1,
      "default": -1
    },
    "bytea_columns": [
      "tx_hash",
      "to_address",
      "from_address"
    ],
    "value_conversion": {
      "gas_price": 1000000000000000000,
      "value": 1000000000000000000
    },
    "address_columns": [
      "to_address"
    ]
  },
  "polygon_zkevm": {
    "column_mapping": {
      "blockNumber": "block_number",
      "transactionHash": "tx_hash",
      "from": "from_address",
      "to": "to_address",
      "gasPrice": "gas_price",
      "effectiveGasPrice": "effective_gas_price",
      "gas": "gas_limit",
      "gasUsed": "gas_used",
      "value": "value",
      "status": "status",
      "input": "empty_input",
      "block_timestamp": "block_timestamp",
      "contractAddress": "receipt_contract_address",
      "type": "tx_type",
      "nonce": "nonce"
    },
    "numeric_columns": [
      "gas_price",
      "gas_used"
    ],
    "special_operations": [
      "handle_effective_gas_price",
      "handle_tx_hash_polygon_zkevm",
      "calculate_tx_fee",
      "convert_input_to_boolean"
    ],
    "date_columns": {
      "block_timestamp": "s"
    },
    "status_mapping": {
      "0": 0,
      "1": 1,
      "default": -1
    },
    "bytea_columns": [
      "tx_hash",
      "to_address",
      "from_address",
      "receipt_contract_address"
    ],
    "value_conversion": {
      "gas_price": 1000000000000000000,
      "value": 1000000000000000000
    },
    "address_columns": [
      "to_address",
      "receipt_contract_address"
    ]
  },
  "zksync_era": {
    "column_mapping": {
      "blockNumber": "block_number",
      "hash": "tx_hash",
      "from": "from_address",
      "to": "to_address",
      "gasPrice": "gas_price",
      "gas": "gas_limit",
      "gasUsed": "gas_used",
      "value": "value",
      "status": "status",
      "input": "empty_input",
      "block_timestamp": "block_timestamp",
      "type": "tx_type",
      "contractAddress": "receipt_contract_address",
      "nonce": "nonce"
    },
    "numeric_columns": [
      "gas_price",
      "gas_used"
    ],
    "special_operations": [
      "calculate_tx_fee",
      "convert_input_to_boolean"
    ],
    "date_columns": {
      "block_timestamp": "s"
    },
    "status_mapping": {
      "0": 0,
      "1": 1,
      "default": -1
    },
    "bytea_columns": [
      "tx_hash",
      "to_address",
      "from_address",
      "receipt_contract_address"
    ],
    "value_conversion": {
      "gas_price": 1000000000000000000,
      "value": 1000000000000000000
    },
    "address_columns": [
      "to_address",
      "receipt_contract_address"
    ]
  },
  "taiko": {
    "column_mapping": {
      "blockNumber": "block_number",
      "hash": "tx_hash",
      "from": "from_address",
      "to": "to_address",
      "effectiveGasPrice": "gas_price",
      "gas": "gas_limit",
      "gasUsed": "gas_used",
      "value": "value",
      "status": "status",
      "input": "empty_input",
      "block_timestamp": "block_timestamp",
      "type": "tx_type",
      "nonce": "nonce"
    },
    "numeric_columns": [
      "gas_price",
      "gas_used"
    ],
    "special_operations": [
      "calculate_tx_fee",
      "convert_input_to_boolean"
    ],
    "date_columns": {
      "block_timestamp": "s"
    },
    "status_mapping": {
      "0": 0,
      "1": 1,
      "default": -1
    },
    "bytea_columns": [
      "tx_hash",
      "to_address",
      "from_address"
    ],
    "value_conversion": {
      "gas_price": 1000000000000000000,
      "value": 1000000000000000000
    },
    "address_columns": [
      "to_address"
    ]
  },
  "ethereum": {
    "column_mapping": {
      "blockNumber": "block_number",
      "hash": "tx_hash",
      "from": "from_address",
      "to": "to_address",
      "gasPrice": "gas_price",
      "gas": "gas_limit",
      "gasUsed": "gas_used",
      "value": "value",
      "status": "status",
      "input": "input_data",
      "block_timestamp": "block_timestamp",
      "maxFeePerGas": "max_fee_per_gas",
      "maxPriorityFeePerGas": "max_priority_fee_per_gas",
      "type": "tx_type",
      "nonce": "nonce",
      "transactionIndex": "position",
      "baseFeePerGas": "base_fee_per_gas",
      "maxFeePerBlobGas": "max_fee_per_blob_gas"
    },
    "numeric_columns": [
      "gas_price",
      "gas_used",
      "value",
      "max_fee_per_gas",
      "max_priority_fee_per_gas",
      "base_fee_per_gas"
    ],
    "special_operations": [
      "handle_max_fee_per_blob_gas",
      "calculate_priority_fee",
      "convert_input_to_boolean",
      "calculate_tx_fee"
    ],
    "date_columns": {
      "block_timestamp": "s"
    },
    "status_mapping": {
      "0": 0,
      "1": 1,
      "default": -1
    },
    "bytea_columns": [
      "tx_hash",
      "to_address",
      "from_address",
      "input_data"
    ],
    "value_conversion": {
      "gas_price": 1000000000000000000,
      "value": 1000000000000000000,
      "max_fee_per_gas": 1000000000000000000,
      "max_priority_fee_per_gas": 1000000000000000000
    },
    "address_columns": [
      "to_address",
      "from_address"
    ]
  },
  # Polygon PoS (Bor): L1-style fields like 'ethereum' (EIP-1559, no L1 fee), but only empty_input instead of the
  # full calldata to keep the table small (~4M tx/day). Fees are in POL (metadata_gas_token).
  "polygon_pos": {
    "column_mapping": {
      "blockNumber": "block_number",
      "hash": "tx_hash",
      "from": "from_address",
      "to": "to_address",
      "gasPrice": "gas_price",
      "gas": "gas_limit",
      "gasUsed": "gas_used",
      "value": "value",
      "status": "status",
      "input": "empty_input",
      "block_timestamp": "block_timestamp",
      "maxFeePerGas": "max_fee_per_gas",
      "maxPriorityFeePerGas": "max_priority_fee_per_gas",
      "type": "tx_type",
      "nonce": "nonce",
      "transactionIndex": "position",
      "baseFeePerGas": "base_fee_per_gas"
    },
    "numeric_columns": [
      "gas_price",
      "gas_used",
      "value",
      "max_fee_per_gas",
      "max_priority_fee_per_gas",
      "base_fee_per_gas"
    ],
    "special_operations": [
      "calculate_priority_fee",
      "convert_input_to_boolean",
      "calculate_tx_fee"
    ],
    "date_columns": {
      "block_timestamp": "s"
    },
    "status_mapping": {
      "0": 0,
      "1": 1,
      "default": -1
    },
    "bytea_columns": [
      "tx_hash",
      "to_address",
      "from_address"
    ],
    "value_conversion": {
      "gas_price": 1000000000000000000,
      "value": 1000000000000000000,
      "max_fee_per_gas": 1000000000000000000,
      "max_priority_fee_per_gas": 1000000000000000000
    },
    "address_columns": [
      "to_address",
      "from_address"
    ]
  },
  "op_chains": {
    "required_columns": [
      "l1GasUsed",
      "l1GasPrice",
      "l1Fee",
      "l1BaseFeeScalar",
      "l1BlobBaseFee",
      "l1BlobBaseFeeScalar"
    ],
    "fillna_values": {
      "l1_gas_price": "0",
      "l1_fee": "0",
      "l1_gas_used": "0",
      "l1_base_fee_scalar": "0",
      "l1_blob_base_fee": "0",
      "l1_blob_base_fee_scalar": "0"
    },
    "column_mapping": {
      "blockNumber": "block_number",
      "hash": "tx_hash",
      "from": "from_address",
      "to": "to_address",
      "gasPrice": "gas_price",
      "gas": "gas_limit",
      "gasUsed": "gas_used",
      "value": "value",
      "status": "status",
      "input": "empty_input",
      "l1GasUsed": "l1_gas_used",
      "l1GasPrice": "l1_gas_price",
      "l1Fee": "l1_fee",
      "l1BaseFeeScalar": "l1_base_fee_scalar",
      "l1BlobBaseFee": "l1_blob_base_fee",
      "l1BlobBaseFeeScalar": "l1_blob_base_fee_scalar",
      "block_timestamp": "block_timestamp",
      "type": "tx_type",
      "nonce": "nonce"
    },
    "numeric_columns": [
      "gas_price",
      "gas_used",
    ],
    "special_operations": [
      "handle_l1_gas_price",
      "handle_l1_fee",
      "handle_l1_fee_scalar",
      "handle_l1_gas_used",
      "handle_l1_base_fee_scalar",
      "handle_l1_blob_base_fee",
      "handle_l1_blob_base_fee_scalar",
      "calculate_tx_fee",
      "convert_input_to_boolean"
    ],
    "date_columns": {
      "block_timestamp": "s"
    },
    "status_mapping": {
      "0": 0,
      "1": 1,
      "default": -1
    },
    "bytea_columns": [
      "tx_hash",
      "to_address",
      "from_address",
    ],
    "value_conversion": {
      "gas_price": 1000000000000000000,
      "value": 1000000000000000000,
      "l1_gas_price": 1000000000000000000,
      "l1_fee": 1000000000000000000,
      "l1_blob_base_fee": 1000000000000000000
    },
    "address_columns": [
      "to_address"
    ]
  },
  "celo": {
    "required_columns": [
      "l1GasUsed",
      "l1GasPrice",
      "l1Fee",
      "l1BaseFeeScalar",
      "l1BlobBaseFee",
      "l1BlobBaseFeeScalar"
    ],
    "fillna_values": {
      "l1_gas_price": "0",
      "l1_fee": "0",
      "l1_gas_used": "0",
      "l1_base_fee_scalar": "0",
      "l1_blob_base_fee": "0",
      "l1_blob_base_fee_scalar": "0"
    },
    "column_mapping": {
      "blockNumber": "block_number",
      "hash": "tx_hash",
      "from": "from_address",
      "to": "to_address",
      "gasPrice": "gas_price",
      "gas": "gas_limit",
      "gasUsed": "gas_used",
      "value": "value",
      "status": "status",
      "input": "empty_input",
      "l1GasUsed": "l1_gas_used",
      "l1GasPrice": "l1_gas_price",
      "l1Fee": "l1_fee",
      "l1BaseFeeScalar": "l1_base_fee_scalar",
      "l1BlobBaseFee": "l1_blob_base_fee",
      "l1BlobBaseFeeScalar": "l1_blob_base_fee_scalar",
      "block_timestamp": "block_timestamp",
      "feeCurrency": "fee_currency",
      "type": "tx_type",
      "nonce": "nonce"
    },
    "numeric_columns": [
      "gas_price",
      "gas_used",
    ],
    "special_operations": [
      "handle_l1_gas_price",
      "handle_l1_fee",
      "handle_l1_fee_scalar",
      "handle_l1_gas_used",
      "handle_l1_base_fee_scalar",
      "handle_l1_blob_base_fee",
      "handle_l1_blob_base_fee_scalar",
      "calculate_tx_fee",
      "convert_input_to_boolean"
    ],
    "date_columns": {
      "block_timestamp": "s"
    },
    "status_mapping": {
      "0": 0,
      "1": 1,
      "default": -1
    },
    "bytea_columns": [
      "tx_hash",
      "to_address",
      "from_address",
      "fee_currency"
    ],
    "value_conversion": {
      "gas_price": 1000000000000000000,
      "value": 1000000000000000000,
      "l1_gas_price": 1000000000000000000,
      "l1_fee": 1000000000000000000,
      "l1_blob_base_fee": 1000000000000000000
    },
    "address_columns": [
      "to_address"
    ]
  }
}