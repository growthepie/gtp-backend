from src.adapters.adapter_logs import AdapterLogs
from eth_abi.abi import decode
from datetime import timezone
from web3 import Web3
import pandas as pd
import json

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract

class AdapterOLI(AbstractAdapter):
    """
    adapter_params require the following fields
        rpc_url:str - the RPC URL to connect to the blockchain
        eas_address:str - the address of the EAS contract (optional)
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("OLIv2", adapter_params, db_connector)

        # setup web3
        self.w3 = Web3(Web3.HTTPProvider(self.adapter_params['rpc_url']))
        self.schema_chain = self.w3.eth.chain_id # which chain we are extracting from

        # setup logs adapter
        self.adapter_logs = AdapterLogs(self.w3)

        # setup EAS contract instance (address might be different on other chains!)
        abi = [ { "inputs": [ { "internalType": "bytes32", "name": "uid", "type": "bytes32" } ], "name": "getAttestation", "outputs": [ { "components": [ { "internalType": "bytes32", "name": "uid", "type": "bytes32" }, { "internalType": "bytes32", "name": "schema", "type": "bytes32" }, { "internalType": "uint64", "name": "time", "type": "uint64" }, { "internalType": "uint64", "name": "expirationTime", "type": "uint64" }, { "internalType": "uint64", "name": "revocationTime", "type": "uint64" }, { "internalType": "bytes32", "name": "refUID", "type": "bytes32" }, { "internalType": "address", "name": "recipient", "type": "address" }, { "internalType": "address", "name": "attester", "type": "address" }, { "internalType": "bool", "name": "revocable", "type": "bool" }, { "internalType": "bytes", "name": "data", "type": "bytes" } ], "internalType": "struct Attestation", "name": "", "type": "tuple" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "revoker", "type": "address" }, { "internalType": "bytes32", "name": "data", "type": "bytes32" } ], "name": "getRevokeOffchain", "outputs": [ { "internalType": "uint64", "name": "", "type": "uint64" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "uid", "type": "bytes32" } ], "name": "isAttestationValid", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" } ]
        address = adapter_params.get("eas_address", "0x4200000000000000000000000000000000000021")
        self.contract = self.w3.eth.contract(address=address, abi=abi)

        print_init(self.name, self.adapter_params)

    """
    extract_params require the following fields:
        contract_address: str - the contract address to extract logs from
        from_block: int - the starting block number
        to_block: int - the ending block number
        topics: list - list of log topics to filter by
        chunk_size: int - number of blocks to process in each chunk (default on most free rpcs: 1000)
        check_if_valid: bool - check a second time through an rpc call onchain if the attestation is valid (default: False)
    """
    def extract(self, extract_params:dict = None) -> pd.DataFrame:
        # store extracted logs with input information in d
        d = []
        logs = self.adapter_logs.extract(extract_params)
        print(f"Total logs extracted: {len(logs)}")
        for log in logs:
            uid = '0x' + log['data'].hex()
            # check if attestation is valid
            if extract_params.get('check_if_valid', False):
                is_valid = self.contract.functions.isAttestationValid(uid).call()
            else:
                is_valid = True
            if is_valid:
                # read onchain attestation
                r = self.get_attestation_data(uid)
                # decode label data
                label_data = self.decode_label_data(r['data'])
                # append to d
                d.append({
                    'uid': uid,
                    'time': pd.to_datetime(r['time'], unit='s').isoformat(),
                    'attester': r['attester'],
                    'recipient': r['recipient'],
                    'revoked': False,
                    'is_offchain': False,
                    'tx_id': '0x' + log['transactionHash'].hex(),
                    'ipfs_hash': None,
                    'revocation_time': pd.to_datetime(r['revocationTime'], unit='s').isoformat(),
                    'chain_id': label_data['chain_id'],
                    'tags_json': label_data['tags_json'],
                    'raw': None,
                    'last_updated_time': pd.Timestamp.now(tz=timezone.utc).replace(tzinfo=None).isoformat(),
                    'schema_info': str(self.schema_chain) + '_' + r['schema']
                })
        df = pd.DataFrame(d)
        print_extract(self.name, extract_params, df.shape)
        return df

    """
    table_name: str - the name of the table to load data into
    """
    def load(self, df: pd.DataFrame, table_name: str = 'attestations'):
        # add prefix \x to attester, recipient, tx_id, uid columns
        df['attester'] = df['attester'].apply(lambda x: '\\x' + x[2:])
        df['recipient'] = df['recipient'].apply(lambda x: '\\x' + x[2:])
        df['tx_id'] = df['tx_id'].apply(lambda x: '\\x' + x[2:])
        df['uid'] = df['uid'].apply(lambda x: '\\x' + x[2:])
        # set index uid 
        df = df.set_index('uid')
        # upsert into database
        self.db_connector.upsert_table(table_name, df)
        print_load(self.name, {'table': table_name}, df.shape)

    ## ----------------- Helper functions --------------------

    def get_attestation_data(self, UID) -> dict:
        """
        Retrieve attestation data from the OLI contract.
        
        Args:
            UID (str): The unique identifier of the attestation.

        Returns:
            dict: Dictionary containing 'chain_id' and 'tags_json'
        """

        r = self.contract.functions.getAttestation(UID).call()

        r_dict = {
            'uid': '0x' + r[0].hex(),
            'schema': '0x' + r[1].hex(),
            'time': r[2],
            #'expirationTime': r[3],
            'revocationTime': r[4],
            #'refUID': '0x' + r[5].hex(),
            'recipient': r[6],
            'attester': r[7],
            'revocable': r[8],
            'data': r[9].hex()
        }

        return r_dict

    def decode_label_data(self, encoded_hex: str) -> dict:
        """
        Decode label data from the OLI format.
        
        Args:
            encoded_hex (str): Encoded label data (with or without '0x' prefix)

        Returns:
            dict: Dictionary containing 'chain_id' and 'tags_json'
        """
        # Remove '0x' prefix if present
        if encoded_hex.startswith('0x'):
            encoded_hex = encoded_hex[2:]
        
        # Convert hex string to bytes
        encoded_bytes = bytes.fromhex(encoded_hex)
        
        # ABI decode the data
        decoded_data = decode(['string', 'string'], encoded_bytes)
        
        chain_id = decoded_data[0]
        tags_json_str = decoded_data[1]
        
        # Parse JSON string back to dict
        tags_json = json.loads(tags_json_str)
        if not isinstance(tags_json, dict):  # do not index invalid jsons or spam
            tags_json = {}
        
        return {
            'chain_id': chain_id,
            'tags_json': tags_json
        }
    