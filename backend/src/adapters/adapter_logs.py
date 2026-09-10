import pandas as pd
from hexbytes import HexBytes
from src.adapters.abstract_adapters import AbstractAdapter
from web3 import Web3
import json
import re
from typing import Optional

class AdapterLogs(AbstractAdapter):
    """
    w3: Web3 instance connected to a blockchain node
    """
    def __init__(self, w3, contract_abi_json: json=None):
        super().__init__("adapter_logs", None, None)
        self.w3 = w3
        if contract_abi_json is not None:
            from eth_utils import event_abi_to_log_topic
            # create a contract instance with the provided ABI for log decoding
            self.contract = self.w3.eth.contract(abi=contract_abi_json)
            # precompute event signature to event name mapping for decoding logs
            self.event_map = {}
            for event in self.contract.all_events():
                # Get topic0 hash for this event
                topic0 = event_abi_to_log_topic(event.abi).hex()
                self.event_map[topic0] = event

    """
    extract_params require the following fields:
        contract_address: str - the contract address to extract logs from
        from_block: int - the starting block number
        to_block: int - the ending block number
        topics: str or list - topic0 or list of topics (that all have to be included e.g. [topic0, None, topic2]) to filter by
        chunk_size: int - number of blocks to process in each chunk (default on most free rpcs: 1000)
        decode: (optional) bool - whether to decode the logs using the contract ABI (default: False), requires contract_abi_json to be provided in the constructor!
    """
    def extract(self, extract_params:dict):
        # get parameters or defaults
        from_block = extract_params.get('from_block', 0)
        to_block = extract_params.get('to_block', 1000)
        contract_address = extract_params.get('contract_address', None)
        topics = extract_params.get('topics', [])
        chunk_size = extract_params.get('chunk_size', 1000)
        decode = extract_params.get('decode', False)
        
        all_logs = []

        # Loop through block range in chunks
        for chunk_start in range(from_block, to_block + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, to_block)

            # extract logs for this chunk
            logs = self.get_logs(
                start_block=chunk_start,
                end_block=chunk_end,
                contract_address=contract_address,
                topics=topics,
                decode=decode
            )
            
            all_logs.extend(logs)
            #print(f"Fetched {len(logs)} logs from blocks {chunk_start} to {chunk_end}")
        
        return all_logs

    ## ----------------- Helper functions --------------------

    def get_logs(self, start_block: int, end_block: int, contract_address: str = None, topics = None, decode: bool = False):
        """
        Retrieve logs from the blockchain for a specific contract and topic(s).
        
        Parameters:
        
        w3 : Web3
            Web3 instance connected to a blockchain node
        start_block : int
            Starting block number (inclusive)
        end_block : int
            Ending block number (inclusive)
        contract_address : str, optional
            The contract address to filter logs from. If None, gets logs from all contracts.
        topics : str or list, optional
            Event topic(s) to filter for. Can be:
            - A single topic string (topic0)
            - A list of topics [topic0, topic1, topic2, topic3]
            - Use None in list for wildcard positions: [topic0, None, topic2]
            - If None, gets all events (no topic filtering)
        
        Returns:
        
        list
            List of log entries matching the filter criteria
        """
        
        # Create the filter parameters
        filter_params = {
            'fromBlock': start_block,
            'toBlock': end_block
        }
        
        # Add address filter only if provided
        if contract_address is not None:
            filter_params['address'] = Web3.to_checksum_address(contract_address)
        
        # Add topics filter only if provided
        if topics is not None:
            # Normalize topics to a list if it's a string
            if isinstance(topics, str):
                topics = [topics]
            filter_params['topics'] = topics
        
        # Get the logs
        try:
            logs = self.w3.eth.get_logs(filter_params)
        except Exception as e:
            if self._is_block_range_too_large_error(e) and start_block < end_block:
                max_blocks = self._extract_block_range_limit(e)
                if max_blocks is None:
                    max_blocks = (end_block - start_block + 1) // 2

                split_logs = []
                for split_start in range(start_block, end_block + 1, max_blocks):
                    split_end = min(split_start + max_blocks - 1, end_block)
                    split_logs.extend(self.get_logs(
                        start_block=split_start,
                        end_block=split_end,
                        contract_address=contract_address,
                        topics=topics,
                        decode=decode
                    ))
                return split_logs
            raise
        
        # decode logs if requested, requires that contract_abi_json was provided in the constructor
        if decode:
            logs = self.decode_logs(logs)
        return logs

    def _is_block_range_too_large_error(self, error: Exception) -> bool:
        """
        Detect RPC errors from providers that reject eth_getLogs ranges.
        Web3/provider versions expose these as ValueError/Web3RPCError/Exception
        with either a dict payload or the dict stringified in the message.
        """
        payloads = list(getattr(error, "args", ()))
        payloads.append(str(error))

        for payload in payloads:
            if isinstance(payload, dict):
                code = payload.get("code")
                message = str(payload.get("message", ""))
                data = str(payload.get("data", ""))
            else:
                payload_text = str(payload)
                code = -32602 if "-32602" in payload_text else None
                message = payload_text
                data = payload_text

            text = f"{message} {data}".lower()
            if code == -32602 and "block range" in text and re.search(r"too large|maximum", text):
                return True

        return False

    def _extract_block_range_limit(self, error: Exception) -> Optional[int]:
        payloads = list(getattr(error, "args", ()))
        payloads.append(str(error))

        for payload in payloads:
            if isinstance(payload, dict):
                text = f"{payload.get('message', '')} {payload.get('data', '')}"
            else:
                text = str(payload)

            match = re.search(r"maximum\s+(\d+)\s+blocks", text, flags=re.IGNORECASE)
            if match:
                return max(int(match.group(1)), 1)

        return None

    def turn_logs_into_df(self, logs):
        """
        Convert a list of Web3 log AttributeDicts into a pandas DataFrame.

        Each log field becomes a column; HexBytes and nested iterables are converted
        into native Python types for readability.
        """
        if not logs:
            return pd.DataFrame()

        def _convert(value):
            if isinstance(value, HexBytes):
                return '0x' + value.hex()
            if isinstance(value, (list, tuple)):
                return [_convert(v) for v in value]
            if isinstance(value, dict):
                return {k: _convert(v) for k, v in value.items()}
            return value

        normalized_logs = [{k: _convert(v) for k, v in dict(log).items()} for log in logs]
        return pd.DataFrame(normalized_logs)
    
    def decode_logs(self, logs):
        """
        Decode raw logs using the contract ABI provided in the constructor.
        Requires that the event_map was created during initialization.

        Parameters:
        logs : list
            List of raw log entries to decode

        Returns:
        list
            List of decoded log entries with event names and parameters
        """
        # 2. Decode logs using our pre-computed map
        decoded_logs = []
        for log in logs:
            topic0 = log['topics'][0].hex()
            
            # O(1) Lookup
            event_template = self.event_map.get(topic0)
            
            if event_template:
                try:
                    # This turns the raw hex into a clean Python dictionary
                    decoded_log = event_template().process_log(log)
                    decoded_logs.append(decoded_log)
                except Exception as e:
                    print(f"Failed to decode log: {e}")
                    decoded_logs.append(log) # Fallback to raw if decoding fails
            else:
                decoded_logs.append(log) # Event not in our ABI

        return decoded_logs
    
    def get_topic0_by_event_name(self, event_name: str):
        for topic0, event in self.event_map.items():
            if event_name.lower() + '(' in str(event).lower():
                return '0x' + topic0
        return None
