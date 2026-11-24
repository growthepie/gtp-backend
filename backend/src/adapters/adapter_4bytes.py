from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import upload_file_to_s3
from src.misc.helper_functions import print_init
import polars as pl
import requests
import getpass
import json
import os
from eth_utils import keccak
import tempfile
import pickle
from src.adapters.rpc_funcs.utils import create_4byte_lookup, write_4byte_sqlite

class Adapter4Bytes(AbstractAdapter):

    def __init__(self, adapter_params:dict={}):

        super().__init__("4Bytes", adapter_params, {})
        print_init(self.name, self.adapter_params)


    def extract(self, extract_params:dict):

        # where to save 4bytes.parquet file
        self.save_path = extract_params.get('save_path', 'backend/')
        sys_user = getpass.getuser()
        if sys_user == 'ubuntu':
            # we are in ec2 instance
            self.save_path = f'/home/{sys_user}/gtp/{self.save_path}' if not self.save_path or self.save_path[0] != '/' else self.save_path
        else:
            # we are already in backend/ folder
            self.save_path = self.save_path.replace('backend/', '')
            
        self.save_path_parquet = os.path.join(self.save_path, "4bytes.parquet") if self.save_path else "4bytes.parquet"
        self.save_path_lookup = os.path.join(self.save_path, "four_byte_lookup.pkl") if self.save_path else "four_byte_lookup.pkl"

        # only two provider of smart contracts are available here: "sourcify" or "verifieralliance", default is "sourcify"
        self.provider = extract_params.get('provider', 'sourcify')
        if self.provider not in ['sourcify', 'verifieralliance']:
            raise ValueError("Provider must be either 'sourcify' or 'verifieralliance'")
        elif self.provider == 'sourcify':
            # moved from export.sourcify.app (manifest.json) to GCS bucket, files listed via S3-style XML listing
            self.base_url = "https://export.sourcify.dev/"
        else:
            self.base_url = "https://export.verifieralliance.org/"

        ## download verified contracts (incremental, ETag-based cache)
        # Notes:
        # * full export is ~40GB (600 files), but only the newest few files change between runs
        # * per-file signature aggregates are cached in cache_dir together with the source file's ETag;
        #   only new/changed files are downloaded, files no longer listed are dropped from the cache
        # * delete cache_dir to force a full rebuild
        self.cache_dir = extract_params.get('cache_dir', os.path.expanduser(f'~/.cache/gtp_4byte/{self.provider}'))
        os.makedirs(self.cache_dir, exist_ok=True)
        etag_path = os.path.join(self.cache_dir, 'etags.json')
        cached_etags = {}
        if os.path.exists(etag_path):
            with open(etag_path) as fh:
                cached_etags = json.load(fh)

        remote_files = self.get_compiled_contracts_files()  # {path: etag or None}

        # drop cache entries for files that no longer exist upstream
        for f in list(cached_etags):
            if f not in remote_files:
                cached_file = os.path.join(self.cache_dir, os.path.basename(f))
                if os.path.exists(cached_file):
                    os.remove(cached_file)
                del cached_etags[f]
                print(f"Removed {f} from cache, no longer listed upstream.")
        self.save_etags(cached_etags, etag_path)

        to_process = [
            f for f, etag in remote_files.items()
            if etag is None or cached_etags.get(f) != etag or not os.path.exists(os.path.join(self.cache_dir, os.path.basename(f)))
        ]
        print(f"{len(remote_files)} files upstream, {len(remote_files) - len(to_process)} unchanged in cache, {len(to_process)} to download.")

        for f in to_process:
            # Download parquet export
            df = self.download_parquet(f)
            # Collect all signatures first
            all_with_params = []
            # Process all rows and collect signatures
            for row in df.iter_rows(named=True):
                abi = json.loads(row['compilation_artifacts'])['abi']
                if abi == None:
                    continue
                signatures = self.extract_4byte_signatures(abi)
                # Collect data for batch creation
                for four_byte, details in signatures.items():
                    all_with_params.append({
                        '4byte': four_byte, 
                        'signature': details['signature_with_params'], 
                        'count': 1
                    })
            # Create DataFrames and aggregate (empty files are cached too, so they aren't re-downloaded every run)
            schema = {'4byte': pl.Utf8, 'signature': pl.Utf8, 'count': pl.Int64}
            df_with_params = pl.DataFrame(all_with_params, schema=schema).group_by(['4byte', 'signature']).agg(pl.col('count').sum())
            df_with_params = df_with_params.sort('count', descending=True)
            # write atomically, then record etag so an interrupted run resumes cleanly
            cached_file = os.path.join(self.cache_dir, os.path.basename(f))
            df_with_params.write_parquet(cached_file + '.tmp')
            os.replace(cached_file + '.tmp', cached_file)
            cached_etags[f] = remote_files[f]
            self.save_etags(cached_etags, etag_path)
            print(f"Processed {f}: {len(df_with_params)} with params")

        ## combine all cached parquet files into one final file called "4bytes.parquet" & "four_byte_lookup.pkl" and save under self.save_path_parquet & self.save_path_lookup
        files = [os.path.join(self.cache_dir, os.path.basename(f)) for f in remote_files]
        print(f"Combining {len(files)} cached parquet files")
        all_df = []
        for file in files:
            df = pl.read_parquet(file)
            all_df.append(df)
        # Aggregate
        combined_df = pl.concat(all_df).group_by(['4byte', 'signature']).agg(pl.col('count').sum())
        combined_df = combined_df.sort('count', descending=True)
        combined_df = combined_df.with_columns(pl.col('signature').map_elements(self.add_variable_names, return_dtype=pl.Utf8).alias('signature'))
        # remove duplicate signatures with same function names (by grouping 4byte & function, then remove any duplicates with lower count number)
        combined_df = combined_df.with_columns(pl.col("signature").str.split("(").list.get(0).alias("function"))
        combined_df = combined_df.group_by(['4byte', 'function']).first()
        # remove count & function column to reduce size
        combined_df = combined_df.sort('count', descending=True)
        combined_df = combined_df.drop(["count", "function"])
        
        ## write to parquet
        combined_df.write_parquet(self.save_path_parquet)
        print("Created file: 4bytes.parquet")
        
        ## write to pickle file
        four_byte_lookup = create_4byte_lookup(combined_df)
        with open(self.save_path_lookup, "wb") as f:
            pickle.dump(four_byte_lookup, f)
        print("Created file: four_byte_lookup.pkl")

        ## write sqlite version next to the pickle: raw/backfill RPC tasks read this instead of loading the pickle (saves ~1.3 GB RAM per task)
        write_4byte_sqlite(four_byte_lookup, self.save_path_lookup.replace('.pkl', '.sqlite'))
        print("Created file: four_byte_lookup.sqlite")
        
        print(f"Combined results: {len(combined_df)} rows")


    def load(self, load_params:dict):
        # Load the path of where to upload the 4bytes.parquet & four_byte_lookup.pkl file
        s3_path_parquet = load_params.get('s3_path_parquet', 'v1/export/4bytes.parquet')
        s3_path_lookup = load_params.get('s3_path_lookup', 'v1/export/four_byte_lookup.pkl')
        # Get upload secrets
        s3_bucket = load_params.get('S3_CF_BUCKET', None)
        cf_distribution_id = load_params.get('CF_DISTRIBUTION_ID', None)
        if s3_bucket is None or cf_distribution_id is None:
            raise ValueError("S3_CF_BUCKET and CF_DISTRIBUTION_ID must be provided in load_params")
        # Upload to S3 & invalidate
        upload_file_to_s3(s3_bucket, s3_path_parquet, self.save_path_parquet, cf_distribution_id)
        print(f"Uploaded 4bytes.parquet to S3: {s3_path_parquet}")
        # Upload pickle file as well
        upload_file_to_s3(s3_bucket, s3_path_lookup, self.save_path_lookup, cf_distribution_id)
        print(f"Uploaded four_byte_lookup.pkl to S3: {s3_path_lookup}")


#-#-#-#-# Helper Functions #-#-#-#-#

    def save_etags(self, etags, path):
        """Atomically write the {path: etag} cache index"""
        with open(path + '.tmp', 'w') as fh:
            json.dump(etags, fh)
        os.replace(path + '.tmp', path)

    def get_compiled_contracts_files(self):
        """Return {parquet path (relative to base_url): etag} for the configured provider (etag None if unknown)"""
        if self.provider == 'sourcify':
            return self.list_bucket_files('v2/compiled_contracts/')
        return {f: None for f in self.get_master_json()['files']['compiled_contracts']}

    def list_bucket_files(self, prefix):
        """List all .parquet keys under prefix via the S3-compatible XML listing (handles pagination), returns {key: etag}"""
        import xml.etree.ElementTree as ET
        ns = {'s3': 'http://doc.s3.amazonaws.com/2006-03-01'}
        keys, marker = {}, ''
        while True:
            response = requests.get(self.base_url, params={'prefix': prefix, 'marker': marker})
            if response.status_code != 200:
                raise Exception(f"Failed to list files from {self.provider}: {response.status_code}")
            root = ET.fromstring(response.content)
            contents = root.findall('s3:Contents', ns)
            page = [c.findtext('s3:Key', namespaces=ns) for c in contents]
            for c in contents:
                key = c.findtext('s3:Key', namespaces=ns)
                if key.endswith('.parquet'):
                    keys[key] = c.findtext('s3:ETag', default='', namespaces=ns).strip('"') or None
            if root.findtext('s3:IsTruncated', default='false', namespaces=ns) != 'true' or not page:
                break
            marker = root.findtext('s3:NextMarker', default=None, namespaces=ns) or page[-1]
        if not keys:
            raise Exception(f"No parquet files found under {self.base_url}?prefix={prefix}")
        print(f"Found {len(keys)} files under {prefix} from {self.provider}")
        return keys

    def get_master_json(self):
        """Get the master.json from {provider} api endpoint"""
        url = f"{self.base_url}manifest.json"
        response = requests.get(url)
        
        if response.status_code == 200:
            print(f"Successfully fetched {self.provider} master.json")
            return response.json()
        else:
            raise Exception(f"Failed to fetch data from {self.provider}: {response.status_code}")

    def download_parquet(self, path):
        """Download a parquet file from {provider} and return it as a Polars DataFrame."""
        url = self.base_url + path
        print(f"Downloading {url}...")
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to download file: {response.status_code}")
        
        # Use temporary file to avoid naming conflicts
        with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
            tmp_file.write(response.content)
            tmp_file_path = tmp_file.name
        
        try:
            df = pl.read_parquet(tmp_file_path)
            print(f"Downloaded and read {path} successfully.")
            return df
        finally:
            os.remove(tmp_file_path)

    def extract_4byte_signatures(self, abi):
        """Extract 4-byte function signatures from ABI"""
        signatures = {}
        
        def expand_type(param):
            """Expand tuple types to their components"""
            if param['type'].startswith('tuple'):
                # Get the array suffix (e.g., "[]" from "tuple[]")
                array_suffix = param['type'][5:]  # Remove "tuple" prefix
                
                # Expand components
                component_types = []
                for component in param.get('components', []):
                    component_types.append(expand_type(component))
                
                # Return as tuple with array suffix
                return f"({','.join(component_types)}){array_suffix}"
            else:
                return param['type']
        
        def expand_type_with_name(param):
            """Expand tuple types to their components with parameter names"""
            if param['type'].startswith('tuple'):
                # Get the array suffix (e.g., "[]" from "tuple[]")
                array_suffix = param['type'][5:]  # Remove "tuple" prefix
                
                # Expand components with names
                component_types_with_names = []
                for component in param.get('components', []):
                    component_types_with_names.append(f"{expand_type(component)} {component['name']}")
                
                # Return as tuple with array suffix and parameter name
                return f"({','.join(component_types_with_names)}){array_suffix} {param['name']}"
            else:
                return f"{param['type']} {param['name']}"
        
        for item in abi:
            if item.get('type') == 'function':
                function_name = item['name']
                inputs = item.get('inputs', [])
                
                param_types = [expand_type(inp) for inp in inputs]
                param_types_with_names = [expand_type_with_name(inp) for inp in inputs]
                
                signature = f"{function_name}({','.join(param_types)})"
                signature_with_params = f"{function_name}({','.join(param_types_with_names)})"
                
                signature_hash = keccak(text=signature)
                four_byte = '0x' + signature_hash[:4].hex()
                
                signatures[four_byte] = {
                    'signature_with_params': signature_with_params
                }
        
        return signatures 
    
    # fix missing variable names (e.g. from "performUpkeep(bytes ,bytes )" to this "performUpkeep(bytes bytes,bytes bytes_0)")
    def add_variable_names(self, x):
        types_already_used = set()
        # Handle ' ,' cases
        while ' ,' in x:
            pos = x.find(' ,')
            before_space = x[:pos]
            parts = before_space.replace('(', ' ').replace(',', ' ').split()
            base_var_name = parts[-1]  # This is "uint256"
            # Generate a unique variable name
            var_name = base_var_name
            i = 0
            while var_name in types_already_used:
                i += 1
                var_name = f"{base_var_name}_{i}"  # Always build from base_var_name, not var_name
            types_already_used.add(var_name)
            x = x.replace(' ,', f" {var_name},", 1)
        # Handle ' )' cases  
        while ' )' in x:
            pos = x.find(' )')
            before_space = x[:pos]
            parts = before_space.replace('(', ' ').replace(',', ' ').split()
            base_var_name = parts[-1]
            var_name = base_var_name
            i = 0
            while var_name in types_already_used:
                i += 1
                var_name = f"{base_var_name}_{i}"  # Always build from base_var_name
            types_already_used.add(var_name)
            x = x.replace(' )', f" {var_name})", 1)
        return x
    