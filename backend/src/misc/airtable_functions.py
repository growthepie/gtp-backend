### AIRTABLE
import pandas as pd

"""#initialize Airtable instance
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
api = Api(AIRTABLE_API_KEY)
table = api.table(AIRTABLE_BASE_ID, 'Unlabeled Contracts')"""

#-#-# GENERAL AIRTABLE FUNCTIONS #-#-#


def _is_missing_scalar(value):
    """Return True only for scalar missing values (NaN/NaT/None)."""
    # Keep list-like Airtable field values (e.g. linked-record arrays) untouched.
    if not pd.api.types.is_scalar(value):
        return False
    try:
        return bool(pd.isnull(value))
    except (TypeError, ValueError):
        return False

# batch upload df to a table
def push_to_airtable(table, df):

    # convert rows to dictionary
    rows = df.to_dict(orient='records')

    # convert all timestamp columns to string
    for col in df.dtypes[df.dtypes == 'datetime64[ns]'].index:
        for row in rows:
            if pd.notnull(row[col]):
                row[col] = row[col].strftime('%Y-%m-%d %H:%M:%S')

    # convert NaT, nan to None for all columns
    for col in df.dtypes.index:
        for row in rows:
            if _is_missing_scalar(row[col]):
                row[col] = None

    # push contracts to Airtable
    table.batch_create(rows)

    print(f"Pushed {len(rows)} records to Airtable.")

# update records in airtable
def update_airtable(table, df):

    # throw error if id column is not present
    if 'id' not in df.columns:
        raise ValueError('id column is missing in the dataframe.')

    # remove all rows that have NaN in the id column
    df = df.dropna(subset=['id'])

    # extract ids from df
    ids = df['id'].tolist()

    # convert rows to dictionary & remove index column
    df = df.drop(columns=['id'])
    rows = df.to_dict(orient='records')

    # convert all timestamp columns to string
    for col in df.dtypes[df.dtypes == 'datetime64[ns]'].index:
        for row in rows:
            if pd.notnull(row[col]):
                row[col] = row[col].strftime('%Y-%m-%d %H:%M:%S')

    # convert NaT, nan to None for all columns
    for col in df.dtypes.index:
        for row in rows:
            if _is_missing_scalar(row[col]):
                row[col] = None

    # create a list of dictionaries with the id and the fields to update
    c = [{'id': i, 'fields': r} for i, r in zip(ids, rows)]

    # update contracts in Airtable
    table.batch_update(c)

    print(f"Updated {len(rows)} records in Airtable.")

# delete all records from airtable
def clear_all_airtable(table):

    # get all ids (= rows) where the temp_owner_project field is not set (because we don't want to delete these contracts since the labelling is not finished for these)
    ids = [i['id'] for i in table.all() if 'temp_owner_project' not in i['fields'].keys()]

    # api can only handle batches of 10
    for i in range(0, len(ids), 10):
        batch_ids = ids[i:i+10]
        table.batch_delete(batch_ids)

    print(f"Deleted {len(ids)} records from Airtable.")

# delete specific records from airtable
def delete_airtable_ids(table, ids: list):
    
    # api can only handle batches of 10
    for i in range(0, len(ids), 10):
        batch_ids = ids[i:i+10]
        table.batch_delete(batch_ids)

    print(f"Deleted {len(ids)} records from Airtable.")

# read all rows from airtable into a df
def read_airtable(table):

    j = table.all()
    df = pd.DataFrame([{**r['fields'], 'id': r['id']} for r in j])
    
    return df   


#-#-# SPECIFIC AIRTABLE FUNCTIONS #-#-#

# get labelled items from airtable as a df (airtable: Unlabeled Contracts)
def read_all_labeled_contracts_airtable(api, AIRTABLE_BASE_ID, table):

    # get all records from airtable
    df = read_airtable(table)

    # check if anything new was labelled: ONLY ACCOUNTS FOR LABELLED STUFF IF ALSO 'approve' COLUMN WAS SET TO TRUE!
    required_columns = ['approve']
    if not any(col in df.columns for col in required_columns):
        print('no new approved contracts found in airtable.')
        return

    # show only contracts that have been labeled
    required_columns = [col for col in required_columns if col in df.columns]
    df = df.dropna(subset=required_columns, how='all')

    # add all columns if they are missing, as api doesn't return empty columns
    if 'contract_name' not in df.columns:
        df['contract_name'] = ''
    if 'owner_project' not in df.columns:
        df['owner_project'] = None
    if 'usage_category' not in df.columns:
        df['usage_category'] = None
    if 'approve' not in df.columns:
        df['approve'] = None
    if '_comment' not in df.columns:
        df['_comment'] = None
    if 'is_proxy' not in df.columns:
        df['is_proxy'] = None

    # drop not needded columns and clean df
    df = df[['address', 'origin_key', 'contract_name', 'owner_project', 'usage_category', '_comment', 'is_proxy']]

    # origin_key, owner_project and usage_category are lists with 1 element, so we extract the element at index 0
    df['owner_project'] = df[df['owner_project'].notnull()]['owner_project'].apply(lambda x: x[0])
    df['usage_category'] = df[df['usage_category'].notnull()]['usage_category'].apply(lambda x: x[0])
    df['origin_key'] = df[df['origin_key'].notnull()]['origin_key'].apply(lambda x: x[0])

    # convert column ids to text for owner_project & usage_category columns (e.g. recgawzTCg3ALuSR2 -> uniswap)
    if len(df[df["owner_project"].notna()]) > 0: # TODO: move this into a function
        df_owner_projects = read_airtable(api.table(AIRTABLE_BASE_ID, 'OSS Projects'))
        df_owner_projects = df_owner_projects[['id', 'Name']]
        df_owner_projects.set_index('id', inplace=True)
        df['owner_project'] = df[df['owner_project'].notnull()]['owner_project'].apply(lambda x: df_owner_projects.loc[x]['Name'])
    if len(df[df["usage_category"].notna()]) > 0: # TODO: move this into a function
        df_usage_categories = read_airtable(api.table(AIRTABLE_BASE_ID, 'Sub Categories'))
        df_usage_categories = df_usage_categories[['id', 'category_id']]
        df_usage_categories.set_index('id', inplace=True)
        df['usage_category'] = df[df['usage_category'].notnull()]['usage_category'].apply(lambda x: df_usage_categories.loc[x]['category_id'])
    if len(df[df["origin_key"].notna()]) > 0: # TODO: move this into a function
        df_chains = read_airtable(api.table(AIRTABLE_BASE_ID, 'Chains'))
        df_chains = df_chains[['id', 'caip2']]
        df_chains.set_index('id', inplace=True)
        df['chain_id'] = df[df['origin_key'].notnull()]['origin_key'].apply(lambda x: df_chains.loc[x]['caip2'])
        df = df.drop(columns=['origin_key'])

    # convert address to bytes
    df['address'] = df['address'].apply(lambda x: x.replace('0x', '\\x'))

    return df

# get labelled items from Remap Owner Project table as a df (airtable: Remap Owner Project)
def read_all_remap_owner_project(api, AIRTABLE_BASE_ID, table):

    # get all records from airtable
    df = read_airtable(table)

    # check if table is empty
    if df.empty:
        print('no owner projects currently need remapping.')
        return

    # check if all the required_columns have at least one filled out field
    required_columns = ['old_owner_project', 'owner_project']
    if not all(col in df.columns for col in required_columns):
        print('no depreciated owner projects were remapped today.')
        return
    
    # owner_project is a list with 1 element, so we extract the element at index 0
    df['owner_project'] = df[df['owner_project'].notnull()]['owner_project'].apply(lambda x: x[0])

     # convert column ids to text for owner_project (e.g. recgawzTCg3ALuSR2 -> uniswap)
    if len(df[df["owner_project"].notna()]) > 0: # TODO: move this into a function
        df_owner_projects = read_airtable(api.table(AIRTABLE_BASE_ID, 'OSS Projects'))
        df_owner_projects = df_owner_projects[['id', 'Name']]
        df_owner_projects.set_index('id', inplace=True)
        df['owner_project'] = df[df['owner_project'].notnull()]['owner_project'].apply(lambda x: df_owner_projects.loc[x]['Name'])

    # drop all columns that are not needed
    df = df[required_columns]

    # drop all rows that have only one column filled out
    df = df.dropna()

    return df

# get approved labels from untrusted owner_project airtable (airtable: Label Pool Reattest)
def read_all_label_pool_reattest(api, AIRTABLE_BASE_ID, table, approved=True):

    # get all records from airtable
    df = read_airtable(table)

    # return if empty
    if df.empty:
        print('no contracts marked for reattesting.')
        return
    
    if 'approve' not in df.columns and approved:
        print('no contracts marked for reattesting with approve column set to true.')
        return

    # add all columns if they are missing, as api doesn't return empty columns
    if 'contract_name' not in df.columns:
        df['contract_name'] = ''
    if 'owner_project' not in df.columns:
        df['owner_project'] = None
    if 'usage_category' not in df.columns:
        df['usage_category'] = None

    # only keep rows where approve is set to true
    if approved:
        df = df[df['approve'] == True]
    
    # drop not needded columns
    df = df[['address', 'origin_key', 'contract_name', 'owner_project', 'usage_category', 'attester', 'id']]

    # origin_key, owner_project and usage_category are lists with 1 element, so we extract the element at index 0
    df['owner_project'] = df[df['owner_project'].notnull()]['owner_project'].apply(lambda x: x[0])
    df['usage_category'] = df[df['usage_category'].notnull()]['usage_category'].apply(lambda x: x[0])
    df['origin_key'] = df[df['origin_key'].notnull()]['origin_key'].apply(lambda x: x[0])

    # convert column ids to text for owner_project & usage_category columns (e.g. recgawzTCg3ALuSR2 -> uniswap)
    if len(df[df["owner_project"].notna()]) > 0: # TODO: move this into a function
        df_owner_projects = read_airtable(api.table(AIRTABLE_BASE_ID, 'OSS Projects'))
        df_owner_projects = df_owner_projects[['id', 'Name']]
        df_owner_projects.set_index('id', inplace=True)
        df['owner_project'] = df[df['owner_project'].notnull()]['owner_project'].apply(lambda x: df_owner_projects.loc[x]['Name'])
    if len(df[df["usage_category"].notna()]) > 0: # TODO: move this into a function
        df_usage_categories = read_airtable(api.table(AIRTABLE_BASE_ID, 'Sub Categories'))
        df_usage_categories = df_usage_categories[['id', 'category_id']]
        df_usage_categories.set_index('id', inplace=True)
        df['usage_category'] = df[df['usage_category'].notnull()]['usage_category'].apply(lambda x: df_usage_categories.loc[x]['category_id'])
    if len(df[df["origin_key"].notna()]) > 0: # TODO: move this into a function
        df_chains = read_airtable(api.table(AIRTABLE_BASE_ID, 'Chains'))
        df_chains = df_chains[['id', 'caip2']]
        df_chains.set_index('id', inplace=True)
        df['chain_id'] = df[df['origin_key'].notnull()]['origin_key'].apply(lambda x: df_chains.loc[x]['caip2'])
        df = df.drop(columns=['origin_key'])

    # convert address to bytes
    df['address'] = df['address'].str.replace('0x', '\\x', regex=False)
    df['attester'] = df['attester'].str.replace('0x', '\\x', regex=False)

    return df
