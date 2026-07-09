import base64
import time
import pandas as pd
import json

# imports for fetching and parsing URIs
import requests 
from urllib.parse import urlparse
import ipaddress
import socket

from src.adapters.abstract_adapters import AbstractAdapter
from src.adapters.adapter_logs import AdapterLogs

from web3 import Web3


class EIP8004Adapter(AbstractAdapter):
    def __init__(self, adapter_params: dict, db_connector):
        super().__init__("EIP-8004", adapter_params, db_connector)
        self.adapter_params = adapter_params

        # contract addresses are the same across all evm chains
        self.address_reputation = "0x8004BAa17C55a88189AE136b182e5fdA19dE9b63"
        self.abi_reputation = json.loads('[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[{"internalType":"address","name":"target","type":"address"}],"name":"AddressEmptyCode","type":"error"},{"inputs":[{"internalType":"address","name":"implementation","type":"address"}],"name":"ERC1967InvalidImplementation","type":"error"},{"inputs":[],"name":"ERC1967NonPayable","type":"error"},{"inputs":[],"name":"FailedCall","type":"error"},{"inputs":[],"name":"InvalidInitialization","type":"error"},{"inputs":[],"name":"NotInitializing","type":"error"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"OwnableInvalidOwner","type":"error"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"OwnableUnauthorizedAccount","type":"error"},{"inputs":[],"name":"UUPSUnauthorizedCallContext","type":"error"},{"inputs":[{"internalType":"bytes32","name":"slot","type":"bytes32"}],"name":"UUPSUnsupportedProxiableUUID","type":"error"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":true,"internalType":"address","name":"clientAddress","type":"address"},{"indexed":true,"internalType":"uint64","name":"feedbackIndex","type":"uint64"}],"name":"FeedbackRevoked","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint64","name":"version","type":"uint64"}],"name":"Initialized","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":true,"internalType":"address","name":"clientAddress","type":"address"},{"indexed":false,"internalType":"uint64","name":"feedbackIndex","type":"uint64"},{"indexed":false,"internalType":"int128","name":"value","type":"int128"},{"indexed":false,"internalType":"uint8","name":"valueDecimals","type":"uint8"},{"indexed":true,"internalType":"string","name":"indexedTag1","type":"string"},{"indexed":false,"internalType":"string","name":"tag1","type":"string"},{"indexed":false,"internalType":"string","name":"tag2","type":"string"},{"indexed":false,"internalType":"string","name":"endpoint","type":"string"},{"indexed":false,"internalType":"string","name":"feedbackURI","type":"string"},{"indexed":false,"internalType":"bytes32","name":"feedbackHash","type":"bytes32"}],"name":"NewFeedback","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":true,"internalType":"address","name":"clientAddress","type":"address"},{"indexed":false,"internalType":"uint64","name":"feedbackIndex","type":"uint64"},{"indexed":true,"internalType":"address","name":"responder","type":"address"},{"indexed":false,"internalType":"string","name":"responseURI","type":"string"},{"indexed":false,"internalType":"bytes32","name":"responseHash","type":"bytes32"}],"name":"ResponseAppended","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"implementation","type":"address"}],"name":"Upgraded","type":"event"},{"inputs":[],"name":"UPGRADE_INTERFACE_VERSION","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address","name":"clientAddress","type":"address"},{"internalType":"uint64","name":"feedbackIndex","type":"uint64"},{"internalType":"string","name":"responseURI","type":"string"},{"internalType":"bytes32","name":"responseHash","type":"bytes32"}],"name":"appendResponse","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"name":"getClients","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getIdentityRegistry","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address","name":"clientAddress","type":"address"}],"name":"getLastIndex","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address","name":"clientAddress","type":"address"},{"internalType":"uint64","name":"feedbackIndex","type":"uint64"},{"internalType":"address[]","name":"responders","type":"address[]"}],"name":"getResponseCount","outputs":[{"internalType":"uint64","name":"count","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address[]","name":"clientAddresses","type":"address[]"},{"internalType":"string","name":"tag1","type":"string"},{"internalType":"string","name":"tag2","type":"string"}],"name":"getSummary","outputs":[{"internalType":"uint64","name":"count","type":"uint64"},{"internalType":"int128","name":"summaryValue","type":"int128"},{"internalType":"uint8","name":"summaryValueDecimals","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getVersion","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"pure","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"int128","name":"value","type":"int128"},{"internalType":"uint8","name":"valueDecimals","type":"uint8"},{"internalType":"string","name":"tag1","type":"string"},{"internalType":"string","name":"tag2","type":"string"},{"internalType":"string","name":"endpoint","type":"string"},{"internalType":"string","name":"feedbackURI","type":"string"},{"internalType":"bytes32","name":"feedbackHash","type":"bytes32"}],"name":"giveFeedback","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"identityRegistry_","type":"address"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"proxiableUUID","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address[]","name":"clientAddresses","type":"address[]"},{"internalType":"string","name":"tag1","type":"string"},{"internalType":"string","name":"tag2","type":"string"},{"internalType":"bool","name":"includeRevoked","type":"bool"}],"name":"readAllFeedback","outputs":[{"internalType":"address[]","name":"clients","type":"address[]"},{"internalType":"uint64[]","name":"feedbackIndexes","type":"uint64[]"},{"internalType":"int128[]","name":"values","type":"int128[]"},{"internalType":"uint8[]","name":"valueDecimals","type":"uint8[]"},{"internalType":"string[]","name":"tag1s","type":"string[]"},{"internalType":"string[]","name":"tag2s","type":"string[]"},{"internalType":"bool[]","name":"revokedStatuses","type":"bool[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address","name":"clientAddress","type":"address"},{"internalType":"uint64","name":"feedbackIndex","type":"uint64"}],"name":"readFeedback","outputs":[{"internalType":"int128","name":"value","type":"int128"},{"internalType":"uint8","name":"valueDecimals","type":"uint8"},{"internalType":"string","name":"tag1","type":"string"},{"internalType":"string","name":"tag2","type":"string"},{"internalType":"bool","name":"isRevoked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"uint64","name":"feedbackIndex","type":"uint64"}],"name":"revokeFeedback","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"upgradeToAndCall","outputs":[],"stateMutability":"payable","type":"function"}]')
        self.address_identity = "0x8004A169FB4a3325136EB29fA0ceB6D2e539a432"
        self.abi_identity = json.loads('[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"inputs":[{"internalType":"address","name":"target","type":"address"}],"name":"AddressEmptyCode","type":"error"},{"inputs":[{"internalType":"address","name":"implementation","type":"address"}],"name":"ERC1967InvalidImplementation","type":"error"},{"inputs":[],"name":"ERC1967NonPayable","type":"error"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"address","name":"owner","type":"address"}],"name":"ERC721IncorrectOwner","type":"error"},{"inputs":[{"internalType":"address","name":"operator","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ERC721InsufficientApproval","type":"error"},{"inputs":[{"internalType":"address","name":"approver","type":"address"}],"name":"ERC721InvalidApprover","type":"error"},{"inputs":[{"internalType":"address","name":"operator","type":"address"}],"name":"ERC721InvalidOperator","type":"error"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"ERC721InvalidOwner","type":"error"},{"inputs":[{"internalType":"address","name":"receiver","type":"address"}],"name":"ERC721InvalidReceiver","type":"error"},{"inputs":[{"internalType":"address","name":"sender","type":"address"}],"name":"ERC721InvalidSender","type":"error"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ERC721NonexistentToken","type":"error"},{"inputs":[],"name":"FailedCall","type":"error"},{"inputs":[],"name":"InvalidInitialization","type":"error"},{"inputs":[],"name":"NotInitializing","type":"error"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"OwnableInvalidOwner","type":"error"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"OwnableUnauthorizedAccount","type":"error"},{"inputs":[],"name":"UUPSUnauthorizedCallContext","type":"error"},{"inputs":[{"internalType":"bytes32","name":"slot","type":"bytes32"}],"name":"UUPSUnsupportedProxiableUUID","type":"error"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"_fromTokenId","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"_toTokenId","type":"uint256"}],"name":"BatchMetadataUpdate","type":"event"},{"anonymous":false,"inputs":[],"name":"EIP712DomainChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint64","name":"version","type":"uint64"}],"name":"Initialized","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":true,"internalType":"string","name":"indexedMetadataKey","type":"string"},{"indexed":false,"internalType":"string","name":"metadataKey","type":"string"},{"indexed":false,"internalType":"bytes","name":"metadataValue","type":"bytes"}],"name":"MetadataSet","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"_tokenId","type":"uint256"}],"name":"MetadataUpdate","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":false,"internalType":"string","name":"agentURI","type":"string"},{"indexed":true,"internalType":"address","name":"owner","type":"address"}],"name":"Registered","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"uint256","name":"agentId","type":"uint256"},{"indexed":false,"internalType":"string","name":"newURI","type":"string"},{"indexed":true,"internalType":"address","name":"updatedBy","type":"address"}],"name":"URIUpdated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"implementation","type":"address"}],"name":"Upgraded","type":"event"},{"inputs":[],"name":"UPGRADE_INTERFACE_VERSION","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"eip712Domain","outputs":[{"internalType":"bytes1","name":"fields","type":"bytes1"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"version","type":"string"},{"internalType":"uint256","name":"chainId","type":"uint256"},{"internalType":"address","name":"verifyingContract","type":"address"},{"internalType":"bytes32","name":"salt","type":"bytes32"},{"internalType":"uint256[]","name":"extensions","type":"uint256[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"name":"getAgentWallet","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"string","name":"metadataKey","type":"string"}],"name":"getMetadata","outputs":[{"internalType":"bytes","name":"","type":"bytes"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getVersion","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"pure","type":"function"},{"inputs":[],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"agentId","type":"uint256"}],"name":"isAuthorizedOrOwner","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"proxiableUUID","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"register","outputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"agentURI","type":"string"},{"components":[{"internalType":"string","name":"metadataKey","type":"string"},{"internalType":"bytes","name":"metadataValue","type":"bytes"}],"internalType":"struct IdentityRegistryUpgradeable.MetadataEntry[]","name":"metadata","type":"tuple[]"}],"name":"register","outputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"string","name":"agentURI","type":"string"}],"name":"register","outputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"string","name":"newURI","type":"string"}],"name":"setAgentURI","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"address","name":"newWallet","type":"address"},{"internalType":"uint256","name":"deadline","type":"uint256"},{"internalType":"bytes","name":"signature","type":"bytes"}],"name":"setAgentWallet","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"operator","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"},{"internalType":"string","name":"metadataKey","type":"string"},{"internalType":"bytes","name":"metadataValue","type":"bytes"}],"name":"setMetadata","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"agentId","type":"uint256"}],"name":"unsetAgentWallet","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"upgradeToAndCall","outputs":[],"stateMutability":"payable","type":"function"}]')

        # keep track of which chain/rpc is currently being used
        self.current_chain = None
        self.current_rpc = None
        self.list_of_rpcs = None
        self.current_rpc_index = 0


    def extract(self, extract_params: dict):
        self.all_events_implemented = ['URIUpdated', 'Registered', 'ResponseAppended', 'NewFeedback']
        chains = extract_params.get('chains', ['*'])
        events = extract_params.get('events', self.all_events_implemented)
        rpc_map = self.get_origin_keys_with_rpcs()
        
        if chains == ['*']:
            chains = rpc_map['origin_key'].unique().tolist()
        if events == ['*']:
            events = self.all_events_implemented
        print(f"Following chains selected: {chains}")
        print(f"Following events selected: {events}")

        # all data
        df_all = pd.DataFrame()

        # get current progress from db
        df_progress = self.get_df_progress()

        # loop through all chains and rpcs and extract events
        for chain in chains:

            # get df_progress_chain
            df_progress_chain = self.get_df_progress_chain(df_progress, chain)
            #print(f"Progress for {chain}: {df_progress_chain}")

            # get block_date_map for this chain
            block_date_map = self.db_connector.get_first_block_of_the_day_range(chain, df_progress_chain['date'].min().strftime('%Y-%m-%d'))
            print(f"Block date map for {chain}: {block_date_map}")
            self.get_new_w3(chain, rpc_map)
            
            for event in events:
                print(f"- Processing {event}")
                if event in ['URIUpdated', 'Registered']:
                    contract_address = self.address_identity
                    contract_abi = self.abi_identity
                elif event in ['ResponseAppended', 'NewFeedback']:
                    contract_address = self.address_reputation
                    contract_abi = self.abi_reputation

                # loop through available day boundaries, extract logs, aggregate and merge into df_all
                block_dates = list(block_date_map)
                for i, date in enumerate(block_dates[1:], start=1):
                    
                    if pd.Timestamp(date) >= df_progress_chain[df_progress_chain['event'] == event]['date'].values[0]:

                        # extract
                        next_available_date = block_dates[i - 1]
                        missing_day_count = (
                            pd.Timestamp(next_available_date) - pd.Timestamp(date)
                        ).days - 1
                        if missing_day_count > 0:
                            print(
                                f"-- Missing first_block_of_day boundary for {chain} after {date}; "
                                f"using next available boundary {next_available_date} "
                                f"({missing_day_count} skipped day(s))"
                            )
                        from_block = block_date_map[date]
                        to_block = block_date_map[next_available_date] - 1
                        daily_logs = self.extract_logs(chain, rpc_map, contract_abi, contract_address, event, from_block, to_block)
                        print(f"-- Extracting {date} starting from block {from_block} to {to_block}")

                        # aggregate
                        df = self.aggregate_daily_logs(daily_logs, event)
                        df['date'] = date
                        df['origin_key'] = chain

                        # merge df into df_all
                        df_all = pd.concat([df_all, df], ignore_index=True)

        return df_all     
        
    def load(self, df:pd.DataFrame, table=None):
        if table is None:
            table = 'eip8004'
        if df.empty:
            print("No new data to load.")
            return
        df = df.copy()
        # remove null bytes from AI garbage
        df['detail'] = df['detail'].apply(lambda x: self.remove_null_bytes(x) if pd.notna(x) else x)
        # set index
        df = df.set_index(['origin_key', 'event', 'date'])
        self.db_connector.upsert_table(table, df)
        print(f"Loaded {len(df)} rows into {table} table.")


    def extract_uri(self, extract_params: dict):
        """
        Scrapes the data behind the URIs.

        Arguments in extract_params:
        - chains: list of chains to extract URIs for, default is all chains in rpc map
        - df: the df from extract function, if provided to know indexing progress
        - days_back: if df is not provided, how many days back to extract URIs for, default is all (will pull in event "Registered" and "URIUpdated" from db to know which URIs to scrape)
        """
        chains = extract_params.get('chains', ['*'])
        df_extract = extract_params.get('df', None)
        days_back = extract_params.get('days_back', None)
        if chains == ['*']:
            rpc_map = self.get_origin_keys_with_rpcs()
            chains = rpc_map['origin_key'].unique().tolist()

        # getting the latest URIs based on df if provided, otherwise based on days_back or all in db 
        print(f"Following chains selected for URI extraction: {chains}")
        if df_extract is None and days_back is None:
            print("No df provided for URI extraction, extracting all URIs from database.")
            db_progress_uri = self.get_db_progress_uri(chains)
        elif df_extract is None and days_back is not None:
            print(f"No df provided for URI extraction, extracting URIs from database for the last {days_back} days.")
            db_progress_uri = self.get_db_progress_uri(chains, days_back=days_back)
        elif df_extract is not None:
            db_progress_uri = self.get_db_progress_uri_from_df(df_extract, chains)
        else:
            print("Invalid parameters for URI extraction passed, please either provide the df from extract, or specify days_back for extracting URIs.")
            return pd.DataFrame()
        
        data = []
        for index, row in db_progress_uri.iterrows():
            uri = row['uri']
            try:
                info = self.scrape_agent_info(uri)
            except Exception as e:
                print(f"Error scraping {uri}: {e}")
                info = None
            data.append({
                'origin_key': row['origin_key'],
                'agent_id': row['agent_id'],
                'uri_json': json.dumps(info if info is not None else {})
            })
        df = pd.DataFrame(data)
        return df
    
    def load_uri(self, df:pd.DataFrame, table=None):
        if table is None:
            table = 'eip8004_uri'
        if df.empty:
            print("No new uri-data to load.")
            return
        df = df.copy()
        # remove null bytes from AI garbage
        df['uri_json'] = df['uri_json'].apply(lambda x: self.remove_null_bytes(x) if pd.notna(x) else x)
        # set index
        df = df.set_index(['origin_key', 'agent_id'])
        self.db_connector.upsert_table(table, df)
        print(f"Loaded {len(df)} rows into {table} table.")


    #-#-# helper functions #-#-#


    def _call_with_rpc_failover(self, chain: str, rpc_map: pd.DataFrame, call_fn):
        """
        Execute a read-only chain call and rotate through chain RPCs on failures.
        """
        self.get_new_w3(chain, rpc_map)
        max_attempts = len(self.list_of_rpcs)
        last_error = None

        for attempt in range(max_attempts):
            rotate = attempt > 0
            w3 = self.get_new_w3(chain, rpc_map, rotate=rotate)
            try:
                return call_fn(w3)
            except Exception as e:
                last_error = e
                print(
                    f"RPC call failed on chain {chain} with RPC {self.current_rpc} "
                    f"(rpcs {attempt + 1}/{max_attempts}): {e}"
                )

        raise RuntimeError(
            f"RPC call failed on chain {chain} after trying {max_attempts} RPC endpoints."
        ) from last_error

    def extract_logs(self, chain, rpc_map, contract_abi, contract_address, event, from_block, to_block):
        def _call_fn(w3):
            ad = AdapterLogs(w3, contract_abi)
            return ad.extract({
                'contract_address': contract_address,
                'from_block': from_block,
                'to_block': to_block,
                'topics': ad.get_topic0_by_event_name(event),
                'chunk_size': 1000,
                'decode': True
            })

        return self._call_with_rpc_failover(chain, rpc_map, _call_fn)

    def get_new_w3(self, chain: str, rpc_map: pd.DataFrame, rotate: bool = False):
        """
        Create a new Web3 instance for a chain and optionally rotate to the next RPC.
        """
        if self.current_chain != chain or self.list_of_rpcs is None:
            chain_rpcs = rpc_map[rpc_map['origin_key'] == chain]['url'].tolist()
            # remove nulls and duplicates while preserving order
            self.list_of_rpcs = [rpc for rpc in dict.fromkeys(chain_rpcs) if rpc]
            if len(self.list_of_rpcs) == 0:
                raise ValueError(f"No RPCs found for chain {chain}.")

            self.current_chain = chain
            self.current_rpc_index = 0
            self.current_rpc = self.list_of_rpcs[self.current_rpc_index]
            print(f"Switched w3 instance to new chain: {chain} with RPC: {self.current_rpc}")
        elif rotate:
            self.current_rpc_index = (self.current_rpc_index + 1) % len(self.list_of_rpcs)
            self.current_rpc = self.list_of_rpcs[self.current_rpc_index]
            print(f"Rotating w3 instance to new RPC for chain: {chain} to RPC: {self.current_rpc}")

        return Web3(Web3.HTTPProvider(self.current_rpc))
    
    def get_df_progress(self):
        # get current progress for all chains and events from db
        df = self.db_connector.execute_query(f"""
            SELECT
                origin_key,
                event,
                MAX(date) AS date
            FROM public.eip8004
            GROUP BY origin_key, event
        """, load_df=True)
        # turn date into Timestamp and add 1 day
        df['date'] = pd.to_datetime(df['date']) + pd.Timedelta(days=1)
        return df
    
    def get_df_progress_chain(self, df_progress, chain):
        # get current progress for a specific chain, plus add new events if they don't exist yet
        df_chain = df_progress[df_progress['origin_key'] == chain]
        for event in self.all_events_implemented:
            if event not in df_chain['event'].values:
                df_chain = pd.concat([df_chain, pd.DataFrame({
                    'origin_key': [chain],
                    'event': [event],
                    'date': [pd.Timestamp(2026,1,29)] # first time EIP8004 was deployed, might makes sense to adjust over time as contracts are deployed to other chains later.
                })], ignore_index=True)
        return df_chain

    def get_origin_keys_with_rpcs(self):
        # get all origin keys to pull in with all rpcs we have available
        df = self.db_connector.execute_query(f"""
            SELECT
                rpc.url,
                rpc.origin_key
            FROM public.sys_rpc_config rpc
            INNER JOIN public.sys_main_conf mc ON rpc.origin_key = mc.origin_key
            WHERE 
                mc.api_deployment_flag = 'PROD' 
                AND mc.chain_type IN ('L2', 'L1') 
                AND mc.evm_chain_id IS NOT NULL
                AND rpc.url != 'https://eth.merkle.io' -- has gaps in historical data...
        """,
        load_df=True)
        return df

    def aggregate_daily_logs(self, daily_logs, event):
        """
        Aggregate daily logs by event type and return a DataFrame.
        
        Args:
            daily_logs: List of event data
            event: Event type ('NewFeedback', 'ResponseAppended', 'Registered', 'URIUpdated')
        
        Returns:
            pd.DataFrame with columns: event, count, detail
        """
        if event == 'NewFeedback':
            agents = {}
            for log in daily_logs:
                agent_id = log['args']['agentId']
                
                if agent_id not in agents:
                    agents[agent_id] = []
                
                agents[agent_id].append({
                    'feedbackIndex': log['args']['feedbackIndex'],
                    'rating': log['args']['value'],
                    'client': log['args']['clientAddress'],
                    'uri': log['args']['feedbackURI'],
                    'hash': log['transactionHash'].hex(),
                    'tag1': log['args']['tag1'],
                    'tag2': log['args']['tag2']
                })
            
            count = len(daily_logs)
            detail = agents
            
        elif event == 'ResponseAppended':
            agents = {}
            for log in daily_logs:
                agent_id = log['args']['agentId']
                
                if agent_id not in agents:
                    agents[agent_id] = []
                
                agents[agent_id].append({
                    'feedbackIndex': log['args']['feedbackIndex'],
                    'client': log['args']['clientAddress'],
                    'responder': log['args']['responder'],
                    'uri': log['args']['responseURI'],
                    'hash': log['transactionHash'].hex()
                })
            
            count = len(daily_logs)
            detail = agents
            
        elif event == 'Registered':
            agents = {}
            for log in daily_logs:
                agent_id = log['args']['agentId']
                
                if agent_id not in agents:
                    agents[agent_id] = []
                
                agents[agent_id].append({
                    'owner': log['args']['owner'],
                    'uri': log['args']['agentURI'],
                    'hash': log['transactionHash'].hex()
                })
            
            count = len(daily_logs)
            detail = agents
            
        elif event == 'URIUpdated':
            agents = {}
            for log in daily_logs:
                agent_id = log['args']['agentId']
                
                if agent_id not in agents:
                    agents[agent_id] = []
                
                agents[agent_id].append({
                    'updatedBy': log['args']['updatedBy'],
                    'uri': log['args']['newURI'],
                    'hash': log['transactionHash'].hex()
                })
            
            count = len(daily_logs)
            detail = agents
        
        else:
            raise ValueError(f"Unknown event type: {event}")
        
        # Create DataFrame
        df = pd.DataFrame([{
            'event': event,
            'count': count,
            'detail': detail
        }])
        
        return df
                
    def remove_null_bytes(self, obj):
        """Recursively remove null bytes from dict/list/str"""
        if isinstance(obj, dict):
            return {key: self.remove_null_bytes(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.remove_null_bytes(item) for item in obj]
        elif isinstance(obj, str):
            return obj.replace('\x00', '')
        else:
            return obj
        
    #-#-# helper functions to scrape URI #-#-#
        
    def scrape_agent_info(self, uri: str):
        """Scrape agent info from URI - supports base64, IPFS, HTTPS, and JSON strings"""
        if "base64" in uri: # base64 encoded JSON
            return self.decode_base64(uri)
        elif "ipfs" in uri and uri[0] != '{': # ipfs hash
            return self.fetch_ipfs(uri)
        elif uri.startswith('https://') or uri.startswith('http://'): # url
            return self.fetch_http(uri)
        else: # assume it's a raw JSON string
            return json.loads(uri)

    def decode_base64(self, uri: str):
        """Extract and decode base64 JSON from the URI"""
        base64_string = uri.split(',')[1]
        decoded = base64.b64decode(base64_string).decode('utf-8')
        return json.loads(decoded)

    def fetch_ipfs(self, uri: str):
        """Extract IPFS hash and fetch JSON"""
        # Extract hash from either ipfs:// or https://ipfs.io/ipfs/
        if uri.startswith('ipfs://'):
            ipfs_hash = uri.replace('ipfs://', '')
        else:
            ipfs_hash = uri.split('/ipfs/')[-1]
        # Make API call
        url = f'https://ipfs.io/ipfs/{ipfs_hash}'
        time.sleep(1)  # Add delay to avoid rate limits
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    
    def fetch_http(self, uri: str):
        parsed = urlparse(uri)
        ip = ipaddress.ip_address(socket.gethostbyname(parsed.hostname))
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            raise ValueError(f"Blocked internal URL: {uri}")

        response = requests.get(uri, timeout=30, allow_redirects=False)
        response.raise_for_status()

        if len(response.content) > 5 * 1024 * 1024:
            raise ValueError("Response too large (over 5MB)")

        return response.json()
    
    def get_db_progress_uri(self, chains, days_back=None):
        days_back_filter = f"AND date >= CURRENT_DATE - INTERVAL '{days_back} days'" if days_back else ""
        # get current progress for URI extraction from db
        df = self.db_connector.execute_query(f"""
            SELECT
                origin_key,
                agent_id,
                COALESCE(
                    (array_agg(agent_details ORDER BY date DESC) FILTER (WHERE event = 'URIUpdated'))[1],
                    (array_agg(agent_details ORDER BY date DESC) FILTER (WHERE event = 'Registered'))[1]
                )->>'uri' AS uri
            FROM (
                SELECT
                    origin_key,
                    date,
                    event,
                    (jsonb_each(detail)).key::integer AS agent_id,
                    (jsonb_each(detail)).value->-1 AS agent_details
                FROM eip8004
                WHERE 
                    event IN ('URIUpdated', 'Registered')
                    AND origin_key IN ({','.join(f"'{chain}'" for chain in chains)})
                    {days_back_filter}
            ) sub
            GROUP BY origin_key, agent_id
            ORDER BY origin_key, agent_id;
        """, load_df=True)
        return df
    
    def get_db_progress_uri_from_df(self, df, chains):
        # if df is empty, we can stop here
        if df.empty:
            print("Provided df for URI extraction is empty, cannot extract URIs.")
            return pd.DataFrame() # return empty dataframe
        
        # Filter events and chains
        filtered = df[
            (df['event'].isin(['URIUpdated', 'Registered'])) &
            (df['origin_key'].isin(chains))
        ].copy()

        # if filtered is empty, we can stop here
        if filtered.empty:
            print("No events found for the specified chains and events.")
            return pd.DataFrame() # return empty dataframe

        # Explode detail dict into rows: each key = agent_id, value = agent_details
        filtered['detail_expanded'] = filtered['detail'].apply(
            lambda d: {int(k): v for k, v in d.items()} if isinstance(d, dict) else {}
        )
        exploded = filtered.explode('detail_expanded') # expand dict to rows

        # Rebuild as proper rows with agent_id and agent_details
        rows = []
        for _, row in filtered.iterrows():
            if isinstance(row['detail'], dict):
                for k, v in row['detail'].items():
                    agent_details = v[-1] if isinstance(v, list) and len(v) > 0 else v
                    rows.append({
                        'origin_key': row['origin_key'],
                        'date': row['date'],
                        'event': row['event'],
                        'agent_id': int(k),
                        'agent_details': agent_details
                    })

        sub = pd.DataFrame(rows)

        # Sort by date desc so first() gives the newest
        sub = sub.sort_values('date', ascending=False)

        # For each group, prefer URIUpdated over Registered
        def get_uri(group):
            uri_updated = group[group['event'] == 'URIUpdated']
            registered = group[group['event'] == 'Registered']

            if not uri_updated.empty:
                agent_details = uri_updated.iloc[0]['agent_details']
            elif not registered.empty:
                agent_details = registered.iloc[0]['agent_details']
            else:
                return None

            # Extract uri from agent_details
            if isinstance(agent_details, dict):
                return agent_details.get('uri')
            return None

        result = (
            sub.groupby(['origin_key', 'agent_id'])
            .apply(get_uri)
            .reset_index()
            .rename(columns={0: 'uri'})
            .sort_values(['origin_key', 'agent_id'])
        )

        return result
