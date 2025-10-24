from src.adapters.abstract_adapters import AbstractAdapter

class AdapterLogs(AbstractAdapter):
    """
    w3: Web3 instance connected to a blockchain node
    """
    def __init__(self, w3):
        super().__init__("adapter_logs", None, None)
        self.w3 = w3

    """
    extract_params require the following fields:
        contract_address: str - the contract address to extract logs from
        from_block: int - the starting block number
        to_block: int - the ending block number
        topics: list - list of log topics to filter by
        chunk_size: int - number of blocks to process in each chunk (default on most free rpcs: 1000)
    """
    def extract(self, extract_params:dict):
        # get parameters or defaults
        from_block = extract_params.get('from_block', 0)
        to_block = extract_params.get('to_block', 1000)
        contract_address = extract_params.get('contract_address', None)
        topics = extract_params.get('topics', [])
        chunk_size = extract_params.get('chunk_size', 1000)
        
        all_logs = []
        
        # Loop through block range in chunks
        for chunk_start in range(from_block, to_block + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, to_block)
            
            # extract logs for this chunk
            logs = self.get_logs(
                start_block=chunk_start,
                end_block=chunk_end,
                contract_address=contract_address,
                topics=topics
            )
            
            all_logs.extend(logs)
            #print(f"Fetched {len(logs)} logs from blocks {chunk_start} to {chunk_end}")
        
        return all_logs

    ## ----------------- Helper functions --------------------

    def get_logs(self, start_block: int, end_block: int, contract_address: str = None, topics = None):
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
        logs = self.w3.eth.get_logs(filter_params)
        
        return logs
    