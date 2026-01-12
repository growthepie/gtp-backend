from src.adapters.adapter_logs import AdapterLogs
from eth_abi.abi import decode
from datetime import timezone
from web3 import Web3
import pandas as pd
import platform
import json

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import print_init, print_load, print_extract

class AdapterOLIOnchain(AbstractAdapter):
    """
    adapter_params require the following fields
        rpc_url:str - the RPC URL to connect to the blockchain
        eas_address:str - the address of the EAS contract (optional)
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("OLI_onchain", adapter_params, db_connector)

        # setup web3
        self.w3 = Web3(Web3.HTTPProvider(self.adapter_params['rpc_url']))
        self.schema_chain = self.w3.eth.chain_id # which chain we are extracting from

        # setup logs adapter
        self.adapter_logs = AdapterLogs(self.w3)
        # if this script is running on Linux, we are on the backend, then use 'backend/', else ''
        self.additional_folder_structure = 'backend/' if platform.system() == 'Linux' else ''

        # setup EAS contract instance (address might be different on other chains!)
        abi = [ { "inputs": [ { "internalType": "bytes32", "name": "uid", "type": "bytes32" } ], "name": "getAttestation", "outputs": [ { "components": [ { "internalType": "bytes32", "name": "uid", "type": "bytes32" }, { "internalType": "bytes32", "name": "schema", "type": "bytes32" }, { "internalType": "uint64", "name": "time", "type": "uint64" }, { "internalType": "uint64", "name": "expirationTime", "type": "uint64" }, { "internalType": "uint64", "name": "revocationTime", "type": "uint64" }, { "internalType": "bytes32", "name": "refUID", "type": "bytes32" }, { "internalType": "address", "name": "recipient", "type": "address" }, { "internalType": "address", "name": "attester", "type": "address" }, { "internalType": "bool", "name": "revocable", "type": "bool" }, { "internalType": "bytes", "name": "data", "type": "bytes" } ], "internalType": "struct Attestation", "name": "", "type": "tuple" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "address", "name": "revoker", "type": "address" }, { "internalType": "bytes32", "name": "data", "type": "bytes32" } ], "name": "getRevokeOffchain", "outputs": [ { "internalType": "uint64", "name": "", "type": "uint64" } ], "stateMutability": "view", "type": "function" }, { "inputs": [ { "internalType": "bytes32", "name": "uid", "type": "bytes32" } ], "name": "isAttestationValid", "outputs": [ { "internalType": "bool", "name": "", "type": "bool" } ], "stateMutability": "view", "type": "function" } ]
        address = adapter_params.get("eas_address", "0x4200000000000000000000000000000000000021")
        self.contract = self.w3.eth.contract(address=address, abi=abi)

        print_init(self.name, self.adapter_params)

    """
    extract_params require the following fields:
        contract_address: str - (optional) the contract address to extract logs from
        from_block: int - the starting block number (negative number for latest block - x or 'last_run_block')
        to_block: int - the ending block number (or 'latest')
        topics: list - list of log topics to filter by
        chunk_size: int - number of blocks to process in each chunk (default on most free rpcs: 1000)
        check_if_valid: bool - check a second time through an rpc call onchain if the attestation is valid (default: False)
    """
    def extract(self, extract_params:dict = None) -> pd.DataFrame:

        # store schema info
        self.schema_info = str(self.schema_chain) + '__' + extract_params.get('topics', '')[3]

        # get block range if 'latest', 'last_run_block' or negative number
        if extract_params.get('to_block', None) == 'latest':
            extract_params['to_block'] = self.w3.eth.block_number
        if extract_params.get('from_block', None) == 'last_run_block':
            extract_params['from_block'] = self.get_last_run_block(self.schema_info)
            if extract_params['to_block'] <= extract_params['from_block']:
                extract_params['from_block'] = extract_params['to_block'] - 1  # rpc has sync issues or there was a reorg(?)
        elif extract_params.get('from_block', 0) < 0:
            extract_params['from_block'] = extract_params.get('to_block', 0) + extract_params['from_block']
        if extract_params.get('from_block', 0) > extract_params.get('to_block', 0):
            raise ValueError("'from_block' must be less than 'to_block' in extract_params")

        # store extracted logs with input information in d
        d = []
        logs = self.adapter_logs.extract(extract_params)

        # process each log and add context
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
                self.schema_info = str(self.schema_chain) + '__' + r['schema']
                # depending on which schema we are reading, process accordingly
                if r['schema'] == '0xb763e62d940bed6f527dd82418e146a904e62a297b8fa765c9b3e1f0bc6fdd68':  # Labels v1
                    # decode label data
                    label_data = self.decode_label_data(r['data'])
                    # append to d
                    d.append({
                        'uid': uid,
                        'time': pd.to_datetime(r['time'], unit='s').isoformat(),
                        'attester': r['attester'],
                        'recipient': r['recipient'],
                        'revoked': True if extract_params.get('topics', [None])[0] == '0xf930a6e2523c9cc298691873087a740550b8fc85a0680830414c148ed927f615' or r['revocationTime'] > 0 else False,
                        'is_offchain': False,
                        'tx_hash': '0x' + log['transactionHash'].hex(),
                        'ipfs_hash': None,
                        'revocation_time': pd.to_datetime(r['revocationTime'], unit='s').isoformat() if r['revocationTime'] > 0 else None,
                        'raw': None,
                        'last_updated_time': pd.Timestamp.now(tz=timezone.utc).replace(tzinfo=None).isoformat(),
                        'schema_info': self.schema_info,
                        # data fields
                        'chain_id': label_data['chain_id'],
                        'tags_json': label_data['tags_json']
                    })
                elif r['schema'] == '0x9e3f6cfb1f5f8e2f3d3e6a5f4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1a2b3c':  # Labels v2
                    # decode label data
                    label_data = self.decode_label_data(r['data'])
                    # split caip10
                    caip10 = label_data['chain_id']
                    if caip10.count(":") != 2:
                        # invalid caip10 format, max 2x":" allowed
                        print(f"Invalid caip10 format: {caip10} for uid: {uid}. Skipping indexing of this label.")
                    else:
                        chain_namespace, chain_reference, address = caip10.split(":", 2)
                        label_data['chain_id'] = chain_namespace + ':' + chain_reference
                        label_data['recipient'] = address
                    # append to d
                    d.append({
                        'uid': uid,
                        'time': pd.to_datetime(r['time'], unit='s').isoformat(),
                        'attester': r['attester'],
                        'recipient': label_data['recipient'],
                        'revoked': True if extract_params.get('topics', [None])[0] == '0xf930a6e2523c9cc298691873087a740550b8fc85a0680830414c148ed927f615' or r['revocationTime'] > 0 else False,
                        'is_offchain': False,
                        'tx_hash': '0x' + log['transactionHash'].hex(),
                        'ipfs_hash': None,
                        'revocation_time': pd.to_datetime(r['revocationTime'], unit='s').isoformat() if r['revocationTime'] > 0 else None,
                        'raw': None,
                        'last_updated_time': pd.Timestamp.now(tz=timezone.utc).replace(tzinfo=None).isoformat(),
                        'schema_info': self.schema_info,
                        # data fields
                        'chain_id': label_data['chain_id'],
                        'tags_json': label_data['tags_json']
                    })
                elif r['schema'] == '0x6d780a85bfad501090cd82868a0c773c09beafda609d54888a65c106898c363d':  # Trust Lists v1
                    # decode trust list data
                    trust_list_data = self.decode_trust_list_data(r['data'])
                    # append to d
                    d.append({
                        'uid': uid,
                        'time': pd.to_datetime(r['time'], unit='s').isoformat(),
                        'attester': r['attester'],
                        'recipient': r['recipient'],
                        'revoked': True if extract_params.get('topics', [None])[0] == '0xf930a6e2523c9cc298691873087a740550b8fc85a0680830414c148ed927f615' or r['revocationTime'] > 0 else False,
                        'is_offchain': False,
                        'tx_hash': '0x' + log['transactionHash'].hex(),
                        'ipfs_hash': None,
                        'revocation_time': pd.to_datetime(r['revocationTime'], unit='s').isoformat() if r['revocationTime'] > 0 else None,
                        'raw': None,
                        'last_updated_time': pd.Timestamp.now(tz=timezone.utc).replace(tzinfo=None).isoformat(),
                        'schema_info': self.schema_info,
                        # data fields
                        'owner_name': trust_list_data['owner_name'],
                        'attesters': trust_list_data['attesters'],
                        'attestations': trust_list_data['attestations']
                    })
        df = pd.DataFrame(d)

        # print extract info only if there was data extracted
        if not df.empty:
            print_extract(self.name, extract_params, df.shape)

        self.extract_params = extract_params  # store for later use in saving last run block

        return df

    """
    table_name: str - the name of the table to load data into
    """
    def load(self, df: pd.DataFrame, table_name: str = 'attestations'):
        
        if df.empty:
            #print(f"No data to load.")
            pass
        else:
            # add prefix \x to attester, recipient, tx_hash, uid columns
            df['attester'] = df['attester'].apply(lambda x: '\\x' + x[2:])
            df['recipient'] = df['recipient'].apply(lambda x: '\\x' + x[2:])
            df['tx_hash'] = df['tx_hash'].apply(lambda x: '\\x' + x[2:])
            df['uid'] = df['uid'].apply(lambda x: '\\x' + x[2:])
            # set index uid
            df = df.set_index('uid')
            # upsert into database
            self.db_connector.upsert_table(table_name, df)
            print_load(self.name, {'table': table_name}, df.shape)
        
        # save last run block
        self.save_last_run_block(self.schema_info, self.extract_params['from_block'], self.extract_params['to_block'])


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
        print(f"Decoding tags_json: {tags_json_str}")
        try:
            tags_json = json.loads(tags_json_str)
            if not isinstance(tags_json, dict):  # do not index invalid jsons or spam
                tags_json = {}
        except json.JSONDecodeError:
            tags_json = {}
        
        return {
            'chain_id': chain_id,
            'tags_json': tags_json
        }
    
    def decode_trust_list_data(self, encoded_hex: str) -> dict:
        """
        Decode trust list data from the OLI format.
        
        Args:
            encoded_hex (str): Encoded trust list data (with or without '0x' prefix)

        Returns:
            dict: Dictionary containing 'owner_name', 'attesters', and 'attestations'
        """
        # Remove '0x' prefix if present
        if encoded_hex.startswith('0x'):
            encoded_hex = encoded_hex[2:]
        
        # Convert hex string to bytes
        encoded_bytes = bytes.fromhex(encoded_hex)
        
        # ABI decode the data
        decoded_data = decode(['string', 'string', 'string'], encoded_bytes)
        
        owner_name = decoded_data[0]
        attesters = decoded_data[1]
        attestations = decoded_data[2]
        
        # parse JSON strings back to dicts/lists
        try:
            attesters = json.loads(attesters)
            if not isinstance(attesters, list):
                attesters = []
        except:
            attesters = []
        try:
            attestations = json.loads(attestations)
            if not isinstance(attestations, list):
                attestations = []
        except:
            attestations = []

        return {
            'owner_name': owner_name,
            'attesters': attesters,
            'attestations': attestations
        }
    
    def get_last_run_block(self, schema_info) -> int:
        """
        Retrieve the last run block number based on the connected chain from a stored file.
        
        Returns:
            int: The last run block number.
        """
        try:
            with open(f'{self.additional_folder_structure}src/adapters/adapter_oli_onchain_last_run_{schema_info}.txt', 'r') as f:
                content = f.read()
                content = json.loads(content.replace("'", '"'))
                return content.get('to_block', 0)
        except FileNotFoundError:
            return 0

    def save_last_run_block(self, schema_info, from_block: int, to_block: int):
        """
        Save the last run block number based on the connected chain to a stored file.
        
        Args:
            from_block (int): The last run from block number.
            to_block (int): The last run to block number.
        """
        with open(f'{self.additional_folder_structure}src/adapters/adapter_oli_onchain_last_run_{schema_info}.txt', 'w') as f:
            f.write(str(
                {'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
                'from_block': from_block, 
                'to_block': to_block}
            ))