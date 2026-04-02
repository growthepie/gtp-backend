import pandas as pd
from github import Github # pip install PyGithub
import zipfile
import io
import yaml
import requests

from src.adapters.abstract_adapters import AbstractAdapter
from src.misc.helper_functions import send_discord_message, print_init, print_load, print_extract

class AdapterOSO(AbstractAdapter):
    """
    adapter_params require the following fields
        none
    """
    def __init__(self, adapter_params:dict, db_connector):
        super().__init__("OSO", adapter_params, db_connector)
        self.g = Github()
        self.webhook_url = adapter_params['webhook']
        print_init(self.name, self.adapter_params)

    """
    load_params require the following fields:
        
    """
    def extract(self, load_params:dict):
        df = self.extract_oss()

        print_extract(self.name, load_params, df.shape)
        return df 

    def load(self, df:pd.DataFrame):
        tbl_name = 'oli_oss_directory'
        df = df[df['timestamp'].isnull() == False] # only load rows that have a timestamp (i.e. new or updated compared to our current db state)
        self.db_connector.upsert_table(tbl_name, df)
        print_load(self.name, df.shape, tbl_name)

    ## ----------------- Helper functions --------------------

    ## This method loads the projects from oss-directory github
    ## It returns a df with columns: ['name', 'display_name', 'description', 'github', 'websites', 'npm', 'social', 'active', 'source']
    def load_oss_projects(self):
        # Get the repository
        repo_url = "https://github.com/opensource-observer/oss-directory/tree/main/data/projects"
        _, _, _, owner, repo_name, _, branch, *path = repo_url.split('/')
        path = '/'.join(path)
        repo = self.g.get_repo(f"{owner}/{repo_name}")

        # Download oss-directory as ZIP file
        zip_url = f"https://github.com/{owner}/{repo_name}/archive/{branch}.zip"
        response = requests.get(zip_url)
        zip_content = io.BytesIO(response.content)

        # Convert ZIP to df of projects
        df = pd.DataFrame()
        with zipfile.ZipFile(zip_content) as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.yaml') and file_name.startswith(f"{repo_name}-{branch}/{path}"):
                    with zip_ref.open(file_name) as file:
                        content = file.read().decode('utf-8')
                        content = yaml.safe_load(content)
                        df_temp = pd.json_normalize(content)
                        if 'social' in content:
                            df_temp['social'] = [content['social']]
                        df = pd.concat([df, df_temp], ignore_index=True)

        # prepare df_projects for inserting into our db table 'oli_oss_directory'
        df = df.reindex(columns=['name', 'display_name', 'description', 'github', 'websites', 'npm', 'social', 'token_coingecko_api_id'])
        df['active'] = True # project is marked active because it is in the OSS directory
        df['source'] = 'OSS_DIRECTORY'

        return df	

    ## Projects that are in our db (df_active_projects) but not in the export from OSS (df_oss) are dropped projects
    ## These projects will get deactivated in our DB and we send a notifcation in our Discord about it
    def deactivate_dropped_projects(self, df_oss, df_active_projects):
        if df_oss.shape[0] > 1800:
            df_dropped_projects = df_active_projects[~df_active_projects['name'].isin(df_oss['name'])].copy()
            dropped_projects = df_dropped_projects['name'].to_list()
            print(f"...{len(dropped_projects)} projects were dropped since the last sync: {dropped_projects}")

            if len(dropped_projects) > 0:
                self.db_connector.deactivate_projects(dropped_projects)

                send_discord_message(f"OSS projects DROPPED. We might need to update our tag_mapping table for label 'owner_project': {dropped_projects}", self.webhook_url)
            else:
                print("Nothing to deactivate")
        else:
            raise Exception("The number of projects in the OSS export is too low. Something went wrong.")
        
    ## Combine the above functions to get the final df and deactivate dropped projects
    def extract_oss(self):
        ## get latest oss projects from oss-directory Github repo and our active projects in our db
        df_oss = self.load_oss_projects()
        df_db = self.db_connector.get_table('oli_oss_directory')
        df_db = df_db[(df_db['active'] == True) & (df_db['source'] == 'OSS_DIRECTORY')]
        df_db = df_db.drop(columns=['logo_path', 'timestamp'])

        ## deactivate projects in our db that don't appear anymore in the oss-directory
        self.deactivate_dropped_projects(df_oss, df_db)

        ## add timestamp to df_oss if any of the columns changed compared to df_db
        df_oss = self.add_timestamp_for_changes(df_oss, df_db)

        ## identify new projects & send a message in Discord
        df_new_projects = df_oss[~df_oss['name'].isin(df_db['name'])]
        new_projects = df_new_projects['name'].to_list()
        print(f"...{len(new_projects)} projects newly added since the last sync: {new_projects}")
        if len(new_projects) > 0:
            send_discord_message(f"<@874921624720257037> OSS projects newly ADDED: {new_projects}", self.webhook_url)

        ## set index
        df_oss.set_index('name', inplace=True)

        return df_oss
    

    ## This function compares df_oss and df_db and adds a timestamp to rows in df_oss that have changed compared to df_db
    def add_timestamp_for_changes(self, df_oss, df_db):
        # Columns to check if data was updated (excluding 'name' which is the merge key)
        CHECK_COLS = ['display_name', 'description', 'github', 'websites', 'npm', 'social', 'active', 'source', 'token_coingecko_api_id']
        df_oss_temp = df_oss.copy()
        df_db_temp = df_db.copy()
        df_oss_temp[CHECK_COLS] = df_oss_temp[CHECK_COLS].fillna("NAN")
        df_db_temp[CHECK_COLS] = df_db_temp[CHECK_COLS].fillna("NAN")
        merged_df = pd.merge(
            df_oss_temp,
            df_db_temp,
            on='name',
            how='left',
            suffixes=('_oss', '_db')
        )
        changed_mask = pd.concat([
            merged_df[f'{col}_oss'].ne(merged_df[f'{col}_db'])
            for col in CHECK_COLS
        ], axis=1).any(axis=1)
        changed_rows_df = df_oss[df_oss['name'].isin(merged_df.loc[changed_mask, 'name'])]
        # Add timestamp to columns in df_oss which are in updated_names
        updated_names = changed_rows_df['name'].to_list()
        timestamp = int(pd.Timestamp.now().timestamp())
        df_oss['timestamp'] = df_oss['name'].apply(lambda x: timestamp if x in updated_names else None)
        return df_oss
