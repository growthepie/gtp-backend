# octant_v2.py

from src.db_connector import DbConnector
from datetime import datetime
import pandas as pd
import numpy as np
import numpy as np
import os
from datetime import datetime, timedelta
from decimal import Decimal
import simplejson as json
import logging
from sqlalchemy import inspect
import math
from src.misc.helper_functions import upload_json_to_cf_s3, empty_cloudfront_cache

from src.misc.octant_lib.helpers import (
    generate_create_table_sql,
    table_definitions,
    GLM_TOKEN_DECIMALS,
    get_lockeds,
    get_unlockeds,
    get_last_epoch,
    get_epoch_start_end_times,
    get_epoch_projects_metadata,
    get_budgets,
    get_allocations,
    get_rewards,
    fetch_rest,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class OctantV2():
    def __init__(self, s3_bucket, cf_distribution_id, db_connector: DbConnector, api_version, user=None):
        """
        Initializes the OctantV2 class

        Args:
            s3_bucket (str): The name of the S3 bucket
            cf_distribution_id (str): The CloudFront distribution ID
            db_connector (DbConnector): The database connector
            api_version (str): The API version
            is_local (bool): Whether the script is running locally
        """

        logging.info("Initializing OctantV2")

        # Check if the script is running locally (local outputs json files to local_output_dir and creates sqlite database)
        self.is_local = False if user == 'ubuntu' else True

        # Constants
        self.api_version = api_version
        self.s3_bucket = s3_bucket
        self.cf_distribution_id = cf_distribution_id
        self.db_connector = db_connector

        self.latest_locked_balances = None
        self.users_with_locked_balances = None
        self.is_lock_history_loaded = False

        # check if connection is established
        if db_connector.engine is None:
            raise Exception("Database connection not established")

        # # create database tables if they do not exist
        for table_key in table_definitions:
            table_name = table_definitions[table_key]['table_name']
            table_exists = self.table_exists(table_name)
            logging.info(
                f"Table {table_name} exists: {table_exists}")

            if not table_exists:
                logging.info(
                    f"Running table creation for {table_name} table")
                # Use 'sqlite' here to generate the correct table schema
                create_table_sql = generate_create_table_sql(
                    table_key=table_key, db_type='sqlite') 

                # create the table in the database
                self.db_connector.engine.execute(
                    create_table_sql
                )

        self.local_output_dir = f"./output/{api_version}/trackers/octantv2/"
        if self.is_local:
            self.local_output_dir = f"./output/{api_version}/trackers/octantv2/"
            # Create the output directory if it does not exist
            if not os.path.exists(self.local_output_dir):
                os.makedirs(self.local_output_dir)

    def table_exists(self, table_name: str):
        """
        Checks if a table exists in the database

        Args:
            table_name (str): The name of the table to check for

        Returns:
            bool: Whether the table exists in the database
        """
        return inspect(self.db_connector.engine).has_table(table_name)

    def save_to_db(self, df: pd.DataFrame, table_key: str):
        """
        Saves a DataFrame to the specified table in the database

        Args:
            df (pd.DataFrame): The DataFrame to save
            table_key (str): The key of the table definition in the table_definitions dictionary
        """

        table_name = table_definitions[table_key]['table_name']
        column_defs = table_definitions[table_key]['columns']
        columns = [col[0] for col in column_defs]

        # if user column exists, rename to user_address
        if 'user' in df.columns:
            df.rename(columns={'user': 'user_address'}, inplace=True)

        # reduce to only the columns that are in the table definition
        df = df[columns]

        # set index to the id column if it exists
        if 'id' in df.columns:
            df.set_index('id', inplace=True)

        # Ensure all index levels are named
        if df.index.name is None:
            df.index.name = 'index'

        # save the data to the database
        self.db_connector.upsert_table(table_name, df)

    def load_from_db(self, table_key: str):
        """
        Loads a table from the database

        Args:
            table_key (str): The key of the table definition in the table_definitions dictionary

        Returns:
            pd.DataFrame: The DataFrame containing the data from the table
        """
        table_name = table_definitions[table_key]['table_name']
        column_defs = table_definitions[table_key]['columns']
        columns = [col[0] for col in column_defs]

        # load the data from the database
        exec_string = f"""
                SELECT *
                FROM {table_name};
            """

        res = self.db_connector.engine.execute(exec_string).fetchall()

        if columns:
            df = pd.DataFrame(res, columns=columns)
        else:
            df = pd.DataFrame(res)

        # if user_address column exists, rename to user
        if 'user_address' in df.columns:
            df.rename(columns={'user_address': 'user'}, inplace=True)

        # convert the datetime columns to datetime objects, if they exist
        for col in df.columns:
            if 'datetime' in col:
                df[col] = pd.to_datetime(df[col])

        logging.info(f"Loaded {len(df)} rows from {table_name}")

        return df

    ##### FILE HANDLERS #####
    def save_to_json(self, data, path):
        # create directory if not exists
        os.makedirs(os.path.dirname(
            f'output/{self.api_version}/{path}.json'), exist_ok=True)
        # save to file
        with open(f'output/{self.api_version}/{path}.json', 'w') as fp:
            json.dump(data, fp, ignore_nan=True)

    def load_octant_data(self, epoch_info: dict):
        """
        Loads Octant data for a given epoch passed in the epoch_info dictionary

        Args:
            epoch_info (dict): The epoch information dictionary
        """
        logging.info(
            f"Loading Octant data for epoch {epoch_info['epoch']}", epoch_info)

        # get the latest locked balances for all users if it has not been loaded yet
        if self.latest_locked_balances is None:
            # get the latest locked balances for all users
            self.latest_locked_balances = self.get_latest_locked_balances()
            # get the users with locked balances
            self.users_with_locked_balances = self.latest_locked_balances['user'].unique(
            )

        # get the projects metadata for the epoch
        # TODO: Skip getting the projects metadata if it already exists in the database for the epoch
        projects_metadata = get_epoch_projects_metadata(epoch_info)

        # get the budgets for the epoch, we pass the users with locked balances so we can get the upcoming budget for each user in case the allocation has not started yet
        budgets = get_budgets(epoch_info, self.users_with_locked_balances)

        # get the allocations for the epoch
        allocations = get_allocations(epoch_info)

        rewards = get_rewards(epoch_info)

        # convert to dataframes
        projects_metadata_df = pd.DataFrame(projects_metadata)
        budgets_df = pd.DataFrame(budgets)
        allocations_df = pd.DataFrame(allocations)
        rewards_df = pd.DataFrame(rewards)

         # --- OLD PROJECT_KEY LOGIC ---
        # sometimes a project's address will change epoch to epoch, so we need to create a project_key in the projects_metadata_df - we'll use the project's websiteLabel if it exists, otherwise we'll use the project's name
        # projects_metadata_df['project_key'] = projects_metadata_df['websiteLabel'].fillna(
        #     projects_metadata_df['name'])
        # --- END OLD PROJECT_KEY LOGIC ---

        # sometimes a project's address will change epoch to epoch, so we need to create a project_key.
        # We use the project's name as the key if the websiteLabel is missing or contains 'github.com'.
        # Otherwise, we use the websiteLabel.
        use_name_as_key = (
            projects_metadata_df['websiteLabel'].isnull() |
            projects_metadata_df['websiteLabel'].str.contains('github.com', na=False)
        )

        projects_metadata_df['project_key'] = np.where(
            use_name_as_key,                               # Condition
            projects_metadata_df['name'],                  # Value if condition is True
            projects_metadata_df['websiteLabel']           # Value if condition is False
        )

        # rename the profileImage columns and the websiteLabel, websiteUrl, introDescription columns
        projects_metadata_df.rename(
            columns={'profileImageSmall': 'profile_image_small', 'profileImageMedium': 'profile_image_medium', 'profileImageLarge': 'profile_image_large', 'websiteLabel': 'website_label', 'websiteUrl': 'website_url', 'introDescription': 'intro_description'}, inplace=True)

        # we'll add the project_key to the allocations_df so we can join the two DataFrames
        if not allocations_df.empty:
            logging.info(
                "Merging allocations with projects metadata")
            allocations_df = pd.merge(
                allocations_df, projects_metadata_df[['address', 'project_key']], left_on='project', right_on='address', how='left')
        else:
            logging.info(
                f"No allocations for epoch {epoch_info['epoch']}")

        # we'll add the project_key to the rewards_df so we can join the two DataFrames
        if not rewards_df.empty:
            logging.info(
                "Merging rewards with projects metadata")
            rewards_df = pd.merge(
                rewards_df, projects_metadata_df[['address', 'project_key']], left_on='address', right_on='address', how='left')

            if not allocations_df.empty:
                logging.info(
                    "adding donor_count and donor_list columns to the rewards_df")
                # add donor_count and donor_list columns to the rewards_df
                donor_count = allocations_df.groupby(
                    'project_key')['donor'].nunique()
                donor_list = allocations_df.groupby('project_key')['donor'].apply(
                    list)
                rewards_df['donor_count'] = rewards_df['project_key'].map(
                    donor_count)
                rewards_df['donor_list'] = rewards_df['project_key'].map(
                    donor_list)
            else:
                logging.info(
                    f"No allocations for epoch {epoch_info['epoch']}")
                # fill donor_count and donor_list with None if there are no allocations
                rewards_df['donor_count'] = 0
                rewards_df['donor_list'] = [[] for _ in range(len(rewards_df))]

        else:
            logging.info(
                f"No rewards for epoch {epoch_info['epoch']}")

        logging.info(
            "Saving Octant data to the database")
        
        # If using SQLite, convert Decimal columns to float before saving.
        if hasattr(self.db_connector, 'use_sqlite') and self.db_connector.use_sqlite:
            logging.info("Converting Decimal columns to float for SQLite compatibility before saving.")
            if not budgets_df.empty:
                budgets_df['amount'] = budgets_df['amount'].astype(float)
            if not allocations_df.empty:
                allocations_df['amount'] = allocations_df['amount'].astype(float)
            if not rewards_df.empty:
                rewards_df['allocated'] = rewards_df['allocated'].astype(float)
                rewards_df['matched'] = rewards_df['matched'].astype(float)
                rewards_df['total'] = rewards_df['total'].astype(float)

        # Save the final DataFrames to DB
        if not projects_metadata_df.empty:
            self.save_to_db(projects_metadata_df, 'projects_metadata')
        if not budgets_df.empty:
            self.save_to_db(budgets_df, 'user_budgets')
        if not allocations_df.empty:
            self.save_to_db(allocations_df, 'user_allocations')
        if not rewards_df.empty:
            self.save_to_db(rewards_df, 'project_allocations_and_matched_rewards')

    def get_latest_locked_balances(self):
        """
        Gets the latest locked balances for all users

        Returns:
            pd.DataFrame: The latest locked balances for all users
        """
        if not self.is_lock_history_loaded:
            self.load_glm_data()

        lock_history_df = self.load_from_db(
            'user_lock_history')

        # get the latest locked balances for all users
        latest_locked_balances = lock_history_df[lock_history_df['current_total_locked_for_user'] > 0].groupby(
            'user').tail(1).reset_index(drop=True)

        return latest_locked_balances

    def get_week_ago_locked_balances(self):
        """
        Gets the locked balances for all users from a week ago

        Returns:
            pd.DataFrame: The locked balances for all users from a week ago
        """
        if not self.is_lock_history_loaded:
            self.load_glm_data()

        lock_history_df = self.load_from_db(
            'user_lock_history')

        one_week_ago = datetime.now() - timedelta(days=7)

        # filter the lock history to only include transactions from more than a week ago
        lock_history_df = lock_history_df[lock_history_df['datetime']
                                          < one_week_ago]

        # get the latest locked balances for all users
        week_ago_locked_balances = lock_history_df[lock_history_df['current_total_locked_for_user'] > 0].groupby(
            'user').tail(1).reset_index(drop=True)

        return week_ago_locked_balances

    # returns the increase/decrease in total locked GLM as well as the increase/decrease in number of users with locked GLM
    def get_locked_changes(self):
        """
        Gets the changes in total locked GLM and number of users with locked GLM

        Returns:
            dict: The changes in total locked GLM and number of users with locked GLM:
            {
                "now": {
                    "total_locked_glm": Decimal,
                    "num_users_locked_glm": int
                },
                "week_ago": {
                    "total_locked_glm": Decimal,
                    "num_users_locked_glm": int
                },
                "changes": {
                    "total_locked_glm_diff": Decimal,
                    "num_users_locked_glm_diff": int,
                    "total_locked_glm_change": Decimal,
                    "num_users_locked_glm_change": Decimal
                }
            }
        """
        logging.info("Getting locked changes")

        latest_locked_balances = self.get_latest_locked_balances()
        week_ago_locked_balances = self.get_week_ago_locked_balances()

        # get the total locked GLM by getting the current_total_locked for the last row of the latest locked balances
        total_locked_glm = latest_locked_balances['current_total_locked'].iloc[-1]
        week_ago_total_locked_glm = week_ago_locked_balances['current_total_locked'].iloc[-1]

        # get the number of users with locked GLM
        num_users_locked_glm = latest_locked_balances['user'].nunique()
        week_ago_num_users_locked_glm = week_ago_locked_balances['user'].nunique(
        )

        # calculate the changes in total locked GLM and number of users with locked GLM
        total_locked_glm_diff = total_locked_glm - week_ago_total_locked_glm
        num_users_locked_glm_diff = num_users_locked_glm - week_ago_num_users_locked_glm

        logging.info(
            f"Total locked GLM: {total_locked_glm}, Number of users with locked GLM: {num_users_locked_glm}")

        return {
            "now": {
                "total_locked_glm": total_locked_glm,
                "num_users_locked_glm": num_users_locked_glm
            },
            "week_ago": {
                "total_locked_glm": week_ago_total_locked_glm,
                "num_users_locked_glm": week_ago_num_users_locked_glm
            },
            "changes": {
                "total_locked_glm_diff": total_locked_glm_diff,
                "num_users_locked_glm_diff": num_users_locked_glm_diff,
                "total_locked_glm_change": total_locked_glm_diff / week_ago_total_locked_glm if week_ago_total_locked_glm != 0 else None,
                "num_users_locked_glm_change": num_users_locked_glm_diff / week_ago_num_users_locked_glm if week_ago_num_users_locked_glm != 0 else None
            }
        }

    def load_glm_data(self):
        """
        Loads GLM data from the database

        Returns:
            pd.DataFrame: The DataFrame containing the GLM data:
            {
                'id': str,
                'blockNumber': int,
                'timestamp': int,
                'datetime': datetime,
                'user': str,
                'amount': Decimal,
                'amount_float': float,
                'current_total_locked': Decimal,
                'current_total_locked_for_user': Decimal
            }
        """
        logging.info("Loading GLM data")

        # -- get the locked and unlocked transactions
        locked_df = pd.DataFrame(get_lockeds())
        unlocked_df = pd.DataFrame(get_unlockeds())

        # -- add datetime column from timestamp column
        locked_df['datetime'] = pd.to_datetime(
            locked_df['timestamp'], unit='s')
        unlocked_df['datetime'] = pd.to_datetime(
            unlocked_df['timestamp'], unit='s')

        # Convert 'amount' to Decimal and adjust for token decimals
        locked_df['amount'] = locked_df['amount'].apply(
            lambda x: Decimal(x) / (10 ** GLM_TOKEN_DECIMALS))
        unlocked_df['amount'] = unlocked_df['amount'].apply(
            lambda x: -Decimal(x) / (10 ** GLM_TOKEN_DECIMALS))  # directly use negative

        # Ensure both DataFrames have the same columns before concatenation
        locked_df = locked_df[['id', 'blockNumber',
                               'timestamp', 'datetime', 'user', 'amount']]
        unlocked_df = unlocked_df[['id', 'blockNumber',
                                   'timestamp', 'datetime', 'user', 'amount']]

        # Append the unlocked dataframe to the locked dataframe and sort by timestamp
        df = pd.concat([locked_df, unlocked_df], ignore_index=True)
        df = df.sort_values(by='timestamp')

        # Convert 'amount' to float for cumsum calculation
        df['amount_float'] = df['amount'].astype(float)

        # Calculate the current total locked using float for cumsum and convert back to Decimal
        df['current_total_locked'] = df['amount_float'].cumsum().apply(Decimal)

        # Calculate the current total locked for each user using float for cumsum and convert back to Decimal
        df['current_total_locked_for_user'] = df.groupby(
            'user')['amount_float'].cumsum().apply(Decimal)

        # Drop the temporary float column
        df.drop(columns=['amount_float'], inplace=True)

        # rename blockNumber to block_number
        df.rename(columns={'blockNumber': 'block_number'}, inplace=True)

        logging.info(f"Loaded {len(df)} rows of GLM data")

        # If using SQLite, convert Decimal columns to float before saving,
        # as the native SQLite driver does not support Decimal.
        if hasattr(self.db_connector, 'use_sqlite') and self.db_connector.use_sqlite:
            logging.info("Converting Decimal columns to float for SQLite compatibility.")
            decimal_cols = ['amount', 'current_total_locked', 'current_total_locked_for_user']
            for col in decimal_cols:
                if col in df.columns:
                    df[col] = df[col].astype(float)

        # Save the resulting DataFrame to the database
        self.save_to_db(
            df, 'user_lock_history')

        self.is_lock_history_loaded = True

    def get_epoch_info(self, epoch: int):
        """
        Gets the epoch information for the given epoch

        Args:
            epoch (int): The epoch number

        Returns:
            dict: The epoch information dictionary:
            {
                "epoch": int,
                "fromTs": int,
                "toTs": int,
                "fromDatetime": datetime,
                "toDatetime": datetime,
                "allocationStart": datetime,
                "allocationEnd": datetime,
                "has_allocation_started": bool,
                "has_allocation_ended": bool
            }
        """
        epoch_start_end_times = get_epoch_start_end_times(epoch)

        # get the epoch info for the epoch with helper fields
        epoch_info: dict = {
            "epoch": epoch_start_end_times["epoch"],
            "fromTs": epoch_start_end_times["fromTs"],
            "toTs": epoch_start_end_times["toTs"],
            "fromDatetime": epoch_start_end_times["fromDatetime"],
            "toDatetime": epoch_start_end_times["toDatetime"],
            "allocationStart": epoch_start_end_times["toDatetime"],
            "allocationEnd": epoch_start_end_times["toDatetime"] + timedelta(seconds=int(epoch_start_end_times["decisionWindow"])),
            "has_allocation_started": epoch_start_end_times["toDatetime"] < datetime.now(),
            "has_allocation_ended": epoch_start_end_times["toDatetime"] + timedelta(seconds=int(epoch_start_end_times["decisionWindow"])) < datetime.now()
        }

        return epoch_info

    def get_median_reward_amount(self, epoch_info: dict = None):
        """
        Gets the median reward amount for the given epoch or all epochs

        Args:
            epoch_info (dict): The epoch information dictionary (default: None to get median for all epochs)

        Returns:
            Decimal: The median reward amount
        """
        rewards_df = self.load_from_db(
            'project_allocations_and_matched_rewards')
        if rewards_df is None:
            # raise Exception("Rewards data not loaded")
            logging.error("Rewards data not loaded")

        if rewards_df.empty:
            # warn that the rewards data is empty
            logging.info(f"No rewards data for epoch {epoch_info['epoch']}")
            return None

        # get the median reward amounts for the given epoch or all epochs
        if epoch_info:
            rewards = rewards_df[rewards_df['epoch'] == epoch_info['epoch']]
        else:
            rewards = rewards_df

        median_reward_amount = rewards['total'].median()

        return median_reward_amount

    def get_locked_glm_per_user(self, epoch_info: dict = None):
        """
        Gets the locked GLM per user for the given epoch or all epochs

        Args:
            epoch_info (dict): The epoch information dictionary (default: None to get locked GLM for all epochs)
        """
        logging.info("Getting locked GLM per user")
        lock_history_df = self.load_from_db(
            'user_lock_history')

        # order the lock history by datetime
        lock_history_df = lock_history_df.sort_values(
            by='datetime', ascending=True)

        if lock_history_df is None or lock_history_df.empty:
            raise Exception("Lock history data not loaded")

        if epoch_info is None:
            # return the min and max locked GLM per user for all epochs
            df = lock_history_df.groupby('user')['current_total_locked_for_user'].agg(
                ['min', 'max'])
            # add the current locked GLM for each user
            df['locked'] = lock_history_df.groupby(
                'user')['current_total_locked_for_user'].last()
            return df

        epoch_start_datetime = epoch_info['fromDatetime']
        epoch_end_datetime = epoch_info['toDatetime']

        # filter data before the epoch start to get pre-epoch locked balances
        pre_epoch_locked_balances = lock_history_df[lock_history_df['datetime']
                                                    < epoch_start_datetime]
        # group by user and get the last locked balance before the epoch start
        pre_epoch_locked_balances = pre_epoch_locked_balances.groupby(
            'user')['current_total_locked_for_user'].last()

        # filter data within the epoch to get locked balances changes during the epoch
        locked_glm_txs = lock_history_df[(lock_history_df['datetime'] >= epoch_start_datetime) &
                                         (lock_history_df['datetime'] <= epoch_end_datetime)]

        # group by user and get the minimum locked balance during the epoch
        min_in_epoch_locked_balances = locked_glm_txs.groupby(
            'user')['current_total_locked_for_user'].min()

        max_in_epoch_locked_balances = locked_glm_txs.groupby(
            'user')['current_total_locked_for_user'].max()

        # combine the pre-epoch locked balances with the minimum locked balances during the epoch into a new dataframe
        # making sure to update users that exist in the pre-epoch locked balances with the minimum locked balances during the epoch, and adding new users that only have locked balances during the epoch
        locked_glm_per_user = pre_epoch_locked_balances.combine_first(
            min_in_epoch_locked_balances)

        # append the pre-epoch and in-epoch locked balances to a dataframe so we can show the min, max, and current locked balances for each user
        df = pd.DataFrame(locked_glm_per_user)
        df = df.rename(columns={'current_total_locked_for_user': 'locked'})
        df['min'] = min_in_epoch_locked_balances
        df['max'] = max_in_epoch_locked_balances

        # fill NaN values with locked balances
        df['min'] = df['min'].fillna(df['locked'])
        df['max'] = df['max'].fillna(df['locked'])

        logging.info(
            f"Minimum locked GLM per user for {'epoch ' + str(epoch_info['epoch']) if epoch_info else 'all epochs'}:")

        return df

    def load_user_data(self, epoch_info: dict):
        """
        Loads per-user data for the given epoch

        Args:
            epoch_info (dict): The epoch information dictionary
        """
        # Get the median reward amount for the epoch
        median_reward_amount = self.get_median_reward_amount(epoch_info)
        locked_glm_per_user = self.get_locked_glm_per_user(epoch_info)
        budgets_df = self.load_from_db(
            'user_budgets')
        allocations_df = self.load_from_db(
            'user_allocations')

        # Initialize result DataFrame
        result_df = locked_glm_per_user.copy()

        # Ensure 'user' is a string type in the result DataFrame for merging purposes
        result_df.index = result_df.index.astype(str)
        result_df.reset_index(inplace=True)
        result_df.rename(columns={'index': 'user'}, inplace=True)

        # Normalize addresses (trim spaces, lowercase)
        result_df['user'] = result_df['user'].astype(str)

        # Handle budgets data
        if budgets_df is None or budgets_df.empty:
            logging.info(
                f"No budgets data for {'epoch ' + str(epoch_info['epoch']) if epoch_info else 'all epochs'}")
            result_df['budget_amount'] = None
        else:
            if epoch_info:
                budgets_df = budgets_df[budgets_df['epoch']
                                        == epoch_info['epoch']]

            # Ensure 'address' is a string type for merging and normalize
            budgets_df['address'] = budgets_df['address'].astype(str)

            # Aggregate budgets by 'address'
            budgets_df = budgets_df.groupby('address', as_index=False)[
                'amount'].sum()

            # Merge with budgets and rename columns appropriately
            result_df = pd.merge(
                result_df,
                budgets_df[['address', 'amount']],
                left_on='user',
                right_on='address',
                how='left'
            )
            result_df.drop(columns=['address'], inplace=True)
            result_df = result_df.rename(columns={'amount': 'budget_amount'})

        # Handle allocations data
        if allocations_df is None or allocations_df.empty:
            logging.info(
                f"No allocations data for {'epoch ' + str(epoch_info['epoch']) if epoch_info else 'all epochs'}")
            result_df['allocation_amount'] = None
            # Set to 0 if no allocations data
            result_df['allocated_to_project_count'] = 0
            # Empty lists for allocated_to_project_keys if no allocations
            result_df['allocated_to_project_keys'] = [[]] * len(result_df)
        else:
            if epoch_info:
                allocations_df = allocations_df[allocations_df['epoch']
                                                == epoch_info['epoch']]

            # Ensure 'donor' is a string type for merging and normalize
            allocations_df['donor'] = allocations_df['donor'].astype(str)

            # Aggregate allocations by 'donor'
            allocations_sum_df = allocations_df.groupby(
                'donor', as_index=False)['amount'].sum()

            # Merge with allocations and rename columns appropriately
            result_df = pd.merge(
                result_df,
                allocations_sum_df[['donor', 'amount']],
                left_on='user',
                right_on='donor',
                how='left'
            )
            result_df.drop(columns=['donor'], inplace=True)
            result_df = result_df.rename(
                columns={'amount': 'allocation_amount'})

            # Calculate the number of projects each donor donated to
            project_count_df = allocations_df.groupby('donor', as_index=False)[
                'project_key'].nunique()
            project_count_df = project_count_df.rename(
                columns={'project_key': 'allocated_to_project_count'})

            # Merge the number of projects each donor donated to
            result_df = pd.merge(
                result_df,
                project_count_df[['donor', 'allocated_to_project_count']],
                left_on='user',
                right_on='donor',
                how='left'
            )
            result_df.drop(columns=['donor'], inplace=True)

            # Calculate the list of project addresses each donor donated to
            project_list_df = allocations_df.groupby(
                'donor')['project_key'].apply(list).reset_index()
            project_list_df = project_list_df.rename(
                columns={'project_key': 'allocated_to_project_keys'})

            # Merge the list of project addresses each donor donated to
            result_df = pd.merge(
                result_df,
                project_list_df[['donor', 'allocated_to_project_keys']],
                left_on='user',
                right_on='donor',
                how='left'
            )
            result_df.drop(columns=['donor'], inplace=True)

        # Fill NaN values with 0 or empty lists
        result_df['budget_amount'] = result_df['budget_amount'].fillna(0)
        result_df['allocation_amount'] = result_df['allocation_amount'].fillna(
            0)
        result_df['allocated_to_project_count'] = result_df['allocated_to_project_count'].fillna(
            0).astype(int)  # Convert to int for counts
        result_df['allocated_to_project_keys'] = result_df['allocated_to_project_keys'].apply(
            lambda x: x if isinstance(x, list) else [])

        # add id column as {epoch_number}_{user_address}
        result_df['id'] = str(epoch_info['epoch']) + '_' + result_df['user']
        # add epoch column
        result_df['epoch'] = epoch_info['epoch']

        # Print final result information
        logging.info(
            f"Median reward amount for {'epoch ' + str(epoch_info['epoch']) if epoch_info else 'all epochs'}: {median_reward_amount}")
        logging.info(
            f"Minimum locked GLM per user for {'epoch ' + str(epoch_info['epoch']) if epoch_info else 'all epochs'}:")

        # Save the final DataFrame to the database
        self.save_to_db(
            result_df, 'user_data')

    def get_oli_projects_with_websites(self):
        """
        Gets the projects JSON from 
        """
        url = "https://api.growthepie.xyz/v1/labels/projects.json"
        projects_json = fetch_rest(url)
        self.save_to_json(projects_json, 'projects')

        oli_projects_metadata = pd.DataFrame(
            projects_json['data']['data'], columns=projects_json['data']['types'])

        # remove rows with no website
        oli_projects_metadata = oli_projects_metadata[oli_projects_metadata['website'].notnull()]

        # remove trailing slashes from the website
        oli_projects_metadata['website'] = oli_projects_metadata['website'].str.rstrip('/')

        # remove duplicate rows based on the website
        oli_projects_metadata = oli_projects_metadata.drop_duplicates(subset='website')

        return oli_projects_metadata

    def create_community_data_json(self):
        """
        Compiles and exports community data for all epochs in a JSON file with the following keys:

            user - contains keys for each user with the following values:\n
                lockeds - keys for each epoch with the last locked balance for the user\n
                mins - keys for each epoch with the minimum locked balance for the user\n
                maxs - keys for each epoch with the maximum locked balance for the user\n
                budget_amounts - keys for each epoch with the total budget amount for the user\n
                allocation_amounts - keys for each epoch with the total allocation amount for the user\n
                allocated_to_project_counts - keys for each epoch with the number of unique projects the user has allocated to\n
                allocated_to_project_keys - keys for each epoch with the list of unique project_keys the user has allocated to\n
        """
        user_data_df = self.load_from_db(
            'user_data')
        
        if hasattr(self.db_connector, 'use_sqlite') and self.db_connector.use_sqlite:
            logging.info("Converting 'allocated_to_project_keys' string back to list for SQLite.")
            # The `json.loads` function will parse the JSON string into a list.
            # We use a try-except block to gracefully handle any empty/malformed strings.
            def to_list(x):
                try:
                    return json.loads(x)
                except (json.JSONDecodeError, TypeError):
                    return [] # Return an empty list if it's not a valid JSON string
            
            user_data_df['allocated_to_project_keys'] = user_data_df['allocated_to_project_keys'].apply(to_list)

        # Initialize the compiled data dictionary
        compiled_data = []

        # Group by 'user' and aggregate the data
        for user, group in user_data_df.groupby('user'):
            row = {
                'user': user,
                'lockeds': group.set_index('epoch')['locked'].to_dict(),
                'mins': group.set_index('epoch')['min'].to_dict(),
                'maxs': group.set_index('epoch')['max'].to_dict(),
                'budget_amounts': group.set_index('epoch')['budget_amount'].to_dict(),
                'allocation_amounts': group.set_index('epoch')['allocation_amount'].to_dict(),
                'allocated_to_project_counts': group.set_index('epoch')['allocated_to_project_count'].to_dict(),
                'allocated_to_project_keys': group.set_index('epoch')['allocated_to_project_keys'].to_dict()
            }
            # convert the value of the allocated_to_project_keys[epoch] to a list
            # if self.db_connector.use_sqlite:
            #     for epoch in row['allocated_to_project_keys']:
            #         row['allocated_to_project_keys'][epoch] = eval(
            #             row['allocated_to_project_keys'][epoch])

            # add "all" key to the lockeds, mins, maxs, budget_amounts, allocation_amounts, allocated_to_project_counts, and allocated_to_project_keys dictionaries

            # locked should be the last value in the lockeds dictionary
            row['lockeds']['all'] = group['locked'].iloc[-1]

            row['mins']['all'] = group['min'].min()
            row['maxs']['all'] = group['max'].max()
            row['budget_amounts']['all'] = group['budget_amount'].sum()
            row['allocation_amounts']['all'] = group['allocation_amount'].sum()

            # for allocated_to_project_counts and allocated_to_project_keys, we'll get the sum of the unique projects and the list of unique projects
            row['allocated_to_project_keys']['all'] = list(
                set([project for project_list in row['allocated_to_project_keys'].values() for project in project_list]))
            row['allocated_to_project_counts']['all'] = len(
                row['allocated_to_project_keys']['all'])

            compiled_data.append(row)
        
        # ## TODO: resolve address to ENS. Not sure if on each runtime is ideal (as it might take a while for 1000 users). Might need to store them in db.
        # for user in compiled_data:
        #     if user['user'] == '0x25854e2a49A6CDAeC7f0505b4179834509038549':
        #         user['user'] = 'Test'

        # save compiled_data
        if self.s3_bucket == None:
            self.save_to_json(compiled_data, '/trackers/octant/community')
            logging.info(
                "/trackers/octant/community.json community JSON saved")
        else:
            upload_json_to_cf_s3(
                self.s3_bucket, f'{self.api_version}/trackers/octant/community', compiled_data, self.cf_distribution_id, invalidate=False)
            logging.info(
                "/trackers/octant/community.json uploaded to S3")

    def create_project_funding_json(self):
        """
        Compiles and exports project funding data for all epochs in a JSON file with the following keys:
            project_key - contains keys for each project with the following values:\n
                allocations - keys for each epoch with the total allocation amount for the project\n
                matched_rewards - keys for each epoch with the total matched rewards for the project\n
                donor_counts - keys for each epoch with the number of unique donors for the project\n
                donor_lists - keys for each epoch with the list of unique donor addresses for the project\n
        """

        project_allocations_df = self.load_from_db(
            'project_allocations_and_matched_rewards')
        
        # If using SQLite, the JSON column is loaded as a string.
        if hasattr(self.db_connector, 'use_sqlite') and self.db_connector.use_sqlite:
            logging.info("Converting 'donor_list' string back to list for SQLite.")
            def to_list(x):
                try:
                    # Filter out None or non-string values before parsing
                    if isinstance(x, str):
                        return json.loads(x)
                    return []
                except (json.JSONDecodeError, TypeError):
                    return []
            
            project_allocations_df['donor_list'] = project_allocations_df['donor_list'].apply(to_list)

        # Initialize the compiled data dictionary
        compiled_data = []

        # Group by 'project_key' and aggregate the data
        for project_key, group in project_allocations_df.groupby('project_key'):
            row = {
                'project_key': project_key,
                'allocations': group.set_index('epoch')['allocated'].to_dict(),
                'matched_rewards': group.set_index('epoch')['matched'].to_dict(),
                'total': group.set_index('epoch')['total'].to_dict(),
                'donor_counts': group.set_index('epoch')['donor_count'].to_dict(),
                'donor_lists': group.set_index('epoch')['donor_list'].to_dict()
            }
            # convert the value of the donor_lists[epoch] to a list
            # if self.db_connector.use_sqlite:
            #     for epoch in row['donor_lists']:
            #         row['donor_lists'][epoch] = eval(row['donor_lists'][epoch])

            # add "all" key to the allocations, matched_rewards, donor_counts, and donor_lists dictionaries
            row['allocations']['all'] = group['allocated'].sum()
            row['matched_rewards']['all'] = group['matched'].sum()
            row['total']['all'] = group['total'].sum()

            # for donor_counts and donor_lists, we'll get the sum of the unique donors and the list of unique donors
            row['donor_lists']['all'] = row['donor_lists']['all'] = list(set([donor for donor_list in row['donor_lists'].values() if donor_list is not None for donor in donor_list]))
            row['donor_counts']['all'] = len(row['donor_lists']['all'])

            # check if any donor_list is None and set to empty list
            for epoch in row['donor_lists']:
                if row['donor_lists'][epoch] is None:
                    row['donor_lists'][epoch] = []

            # check if any donor_counts are nan and set to 0
            for epoch in row['donor_counts']:
                if math.isnan(row['donor_counts'][epoch]):
                    row['donor_counts'][epoch] = 0

            compiled_data.append(row)

        # save compiled_data
        if self.s3_bucket == None:
            self.save_to_json(
                compiled_data, '/trackers/octant/project_funding')
            logging.info(
                "/trackers/octant/project_funding.json funding JSON saved")
        else:
            upload_json_to_cf_s3(
                self.s3_bucket, f'{self.api_version}/trackers/octant/project_funding', compiled_data, self.cf_distribution_id, invalidate=False)
            logging.info(
                "/trackers/octant/project_funding.json uploaded to S3")

    def create_project_metadata_json(self):
        """
        Compiles and exports project metadata for all epochs in a JSON file with the following keys

        project_key - contains keys for each project with the following keys:
            address, cid, name, introDescription, description, profileImageSmall, profileImageMedium, profileImageLarge, websiteLabel, websiteUrl - str
        """
        projects_metadata_df = self.load_from_db('projects_metadata')

        ##fix grqwthepie name
        projects_metadata_df['name'] = projects_metadata_df['name'].replace('GrowThePie', 'growthepie')

        # remove trailing slashes from the websiteUrl
        projects_metadata_df['website_url'] = projects_metadata_df['website_url'].str.rstrip('/')

        oli_metadata = self.get_oli_projects_with_websites()
        # make the website the index
        oli_metadata.set_index('website', inplace=True)
        # Initialize the compiled data dictionary
        compiled_data = {}

        # Group by 'project_key' and aggregate the data
        for project_key, group in projects_metadata_df.groupby('project_key'):
            metadata = group.set_index('epoch').to_dict(orient='index')

            # get the latest websiteUrl for the project
            for epoch in metadata:
                websiteUrl = metadata[epoch]['website_url']
                break

            # get the oli_metadata for the project
            if websiteUrl in oli_metadata.index:
                oli_project_metadata = oli_metadata.loc[websiteUrl]
            else:
                oli_project_metadata = None

            # remove the id, project_key fields
            for epoch in metadata:
                metadata[epoch].pop('id', None)
                metadata[epoch].pop('project_key', None)

                # check if the websiteUrl exists in the oli_metadata
                if oli_project_metadata is not None:
                    metadata[epoch]['main_github'] = oli_project_metadata['main_github']
                    metadata[epoch]['twitter'] = oli_project_metadata['twitter']
                else:
                    metadata[epoch]['main_github'] = None
                    metadata[epoch]['twitter'] = None

            compiled_data[project_key] = metadata

        # save compiled_data
        if self.s3_bucket == None:
            self.save_to_json(
                compiled_data, '/trackers/octant/project_metadata')
            logging.info(
                "/trackers/octant/project_metadata.json metadata JSON saved")
        else:
            upload_json_to_cf_s3(
                self.s3_bucket, f'{self.api_version}/trackers/octant/project_metadata', compiled_data, self.cf_distribution_id, invalidate=False)
            logging.info(
                "/trackers/octant/project_metadata.json uploaded to S3")

    def load_and_create_summary_json(self):
        """
        Loads, compiles, and exports summary data which includes epoch information, locked changes, and median reward amounts for all epochs with the following structure:

        epochs - contains keys for each epoch with the following values:
            epoch, fromTs, toTs, fromDatetime, toDatetime, allocationStart, allocationEnd, has_allocation_started, has_allocation_ended
        locked_changes - contains keys for "now", "week_ago", and "changes" with the following values:
            total_locked_glm, num_users_locked_glm, total_locked_glm_diff, num_users_locked_glm_diff, total_locked_glm_change, num_users_locked_glm_change
        median_reward_amounts - contains keys for each epoch and "all" with the median reward amount for each epoch and all epochs
        """
        locked_changes = self.get_locked_changes()
        last_epoch = get_last_epoch()
        project_allocations_df = self.load_from_db(
            'project_allocations_and_matched_rewards')
        
        ## get total funding amount (sum column 'total' in project_allocations_df)
        total_funding_amount = project_allocations_df['total'].sum()

        # initialize the epoch info list
        compiled_data = {
            'epochs': {},
            'locked_changes': locked_changes,
            'total_funding_amount': total_funding_amount,
        }

        median_reward_amounts = {}

        for epoch in range(last_epoch, 0, -1):
            epoch_info = self.get_epoch_info(epoch)
            # convert the datetime objects to strings
            epoch_info['fromDatetime'] = epoch_info['fromDatetime'].strftime("%Y-%m-%d %H:%M:%S")
            epoch_info['toDatetime'] = epoch_info['toDatetime'].strftime("%Y-%m-%d %H:%M:%S")
            epoch_info['allocationStart'] = epoch_info['allocationStart'].strftime("%Y-%m-%d %H:%M:%S")
            epoch_info['allocationEnd'] = epoch_info['allocationEnd'].strftime("%Y-%m-%d %H:%M:%S")

            # add the epoch info to the data dictionary
            compiled_data['epochs'][epoch] = epoch_info

            # get the median reward amount for the epoch
            median_reward_amount = self.get_median_reward_amount(epoch_info)
            # if NaN, set to None
            median_reward_amounts[epoch] = median_reward_amount if not math.isnan(
                median_reward_amount) else None

        # get the median reward amount for all epochs and add it to the median_reward_amounts dictionary
        median_reward_amounts['all'] = self.get_median_reward_amount()

        # add the median reward amounts to the data dictionary
        compiled_data['median_reward_amounts'] = median_reward_amounts

        # save compiled_data
        if self.s3_bucket == None:
            self.save_to_json(compiled_data, '/trackers/octant/summary')
            logging.info(
                "/trackers/octant/summary.json summary JSON saved")
        else:
            upload_json_to_cf_s3(
                self.s3_bucket, f'{self.api_version}/trackers/octant/summary', compiled_data, self.cf_distribution_id, invalidate=False)
            logging.info(
                "/trackers/octant/summary.json uploaded to S3")
        
        # Only invalidate cache if we have a distribution ID (i.e., not a local run)
        if self.cf_distribution_id:   
            empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/trackers/octant/*')

    def load_epoch_data(self, epoch: int):
        """
        Loads all Octant data for the given epoch into the database

        Args:
            epoch (int): The epoch number
        """
        logging.info(f"Loading Octant data for epoch {epoch}")
        epoch_info = self.get_epoch_info(epoch)
        logging.info(f"Epoch info: {epoch_info}")
        self.load_octant_data(epoch_info)
        logging.info(f"Octant data loaded for epoch {epoch}")
        self.load_user_data(epoch_info)
        logging.info(f"User data loaded for epoch {epoch}")

    def run_load_epoch_data(self, epoch=None):
        """
        Loads all Octant data for the latest epoch into the database.
        If epoch is specified, loads all Octant data for the given epoch into the database.
        Else, loads all Octant data for the latest epoch into the database.
        """
        if epoch is None:
            epoch = get_last_epoch()

        self.load_epoch_data(epoch)

    def run_load_octant_data_for_all_epochs(self):
        """
        Loads all Octant data for all epochs into the database
        """
        last_epoch = get_last_epoch()
        for epoch in range(last_epoch, 0, -1):
            self.load_epoch_data(epoch)

    def run_create_all_octant_jsons(self):
        # loads locked GLM data, epoch info, calculates median reward amounts from data in the DB
        # then creates the summary.json from the data
        logging.info(f"# Creating Octant Summary JSON")
        self.load_and_create_summary_json()

        # creates the community.json from data in the DB
        logging.info(f"# Creating Octant Community JSON")
        self.create_community_data_json()

        # creates the project_funding.json from data in the DB
        logging.info(f"# Creating Octant Project Funding JSON")
        self.create_project_funding_json()

        # creates the project_metadata.json from data in the DB
        logging.info(f"# Creating Octant Project Metadata JSON")
        self.create_project_metadata_json()

        # Only invalidate cache if running on server with a distribution ID
        if self.cf_distribution_id:
            empty_cloudfront_cache(self.cf_distribution_id, f'/{self.api_version}/trackers/octant/*')