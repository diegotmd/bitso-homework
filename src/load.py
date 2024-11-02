import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import util

def read_user_id_dataframe(date):
    
    user_file_path = util.data_lake_file_path(util.user_id_table_name(), 'trusted', date)
    user_df = pd.read_csv(user_file_path)

    user_df['user_id'] = user_df['user_id'].astype(str)

    return user_df

def read_user_level_dataframe(date):
    
    user_level_file_path = util.data_lake_file_path(util.user_level_table_name(), 'trusted', date)

    if not os.path.isfile(user_level_file_path): 
        user_level_df = util.create_empty_dataframe(util.user_level_schema())
    else:
        user_level_df = pd.read_csv(user_level_file_path)
    
    user_level_df['event_timestamp'] = pd.to_datetime(user_level_df['event_timestamp'], format='mixed')
    user_level_df['user_id'] = user_level_df['user_id'].astype(str)

    return user_level_df

def read_fact_user_level_dataframe():
    
    fact_user_level_file_path = 'data-lake/curated/' + util.user_level_table_name() + '.csv'

    if not os.path.isfile(fact_user_level_file_path): 
        fact_user_level_df = util.create_empty_dataframe(util.user_level_schema())
    else:
        fact_user_level_df = pd.read_csv(fact_user_level_file_path)
    
    fact_user_level_df['event_timestamp'] = pd.to_datetime(fact_user_level_df['event_timestamp'], format='mixed')
    fact_user_level_df['user_id'] = fact_user_level_df['user_id'].astype(str)

    return fact_user_level_df

def read_event_dataframe(date):
    
    event_file_path = util.data_lake_file_path(util.event_table_name(), 'trusted', date)
    if not os.path.isfile(event_file_path): 
        event_df = util.create_empty_dataframe(util.event_schema())
    else:
        event_df = pd.read_csv(event_file_path)

    # Convert event_timestamp to datetime
    event_df['event_timestamp'] = pd.to_datetime(event_df['event_timestamp'], format='mixed')
    event_df['user_id'] = event_df['user_id'].astype(str)

    return event_df

def generate_dim_user(date):
    
    user_df = read_user_id_dataframe(date)
    event_df = read_event_dataframe(date)
    
    # Merge the DataFrames
    merged_df = pd.merge(user_df, event_df, on='user_id', how='left')

    # Filter for login events
    login_events = merged_df[merged_df['event_name'] == 'login']

    # Get the latest login for each user
    result_df = login_events.groupby('user_id', as_index=False)['event_timestamp'].max()
    result_df.rename(columns={'event_timestamp': 'last_login'}, inplace=True)

    #result_df['last_login'] = pd.to_datetime(result_df['last_login'], format='mixed')

    return result_df

def merge_dim_user(source_df, destination_df):
    
    merged_df = destination_df.merge(source_df, on='user_id', how='outer', suffixes=('_dest', '_src'))
    merged_df['last_login'] = merged_df[['last_login_dest', 'last_login_src']].max(axis=1)
    result_df = merged_df[['user_id', 'last_login']]
    
    # Drop any duplicate rows if necessary
    result_df.drop_duplicates(subset=['user_id'], inplace=True)
    
    return result_df

def load_dim_user(date):
    
    print('Loading Dim User for ' + date.strftime("%Y-%m-%d"))
    
    new_df = generate_dim_user(date)
    destination_df_path = 'data-lake/curated/' + util.dim_user_table_name() + '.csv'

    # Ensure the directory exists
    os.makedirs(os.path.dirname(destination_df_path), exist_ok=True)

    if os.path.isfile(destination_df_path): 
        destination_df = pd.read_csv(destination_df_path)
        destination_df['last_login'] = pd.to_datetime(destination_df['last_login'], format='mixed')
        final_df = merge_dim_user(new_df, destination_df)
        final_df.to_csv(destination_df_path, index=False)
    else:
        # if dim data file doesn't exist yet (first processing)
        new_df.to_csv(destination_df_path, index=False)


def generate_fact_deposit(date, deposit_table_name, deposit_schema):
    deposit_file_path = util.data_lake_file_path(deposit_table_name, 'trusted', date)

    if not os.path.isfile(deposit_file_path): 
        deposit_df = util.create_empty_dataframe(deposit_schema)
    else:
        deposit_df = pd.read_csv(deposit_file_path)
        
    deposit_df['event_timestamp'] = pd.to_datetime(deposit_df['event_timestamp'], format='mixed')
    deposit_df['user_id'] = deposit_df['user_id'].astype(str)

    return deposit_df

def merge_fact_deposit(date, source_df, destination_df):
    # Convert the date parameter to a Timestamp for comparison
    load_date = pd.to_datetime(date)

    # Filter out rows in destination_df that match the specified date in event_timestamp, so the process is idempotent
    destination_df = destination_df[
        destination_df['event_timestamp'].dt.date != load_date.date()
    ]
    
    # Append new data from source_df to the updated destination_df
    updated_destination_df = pd.concat([destination_df, source_df], ignore_index=True)

    # Remove any duplicates by 'id', keeping the last occurrence (in case of retries)
    updated_destination_df.drop_duplicates(subset='id', keep='last', inplace=True)

    return updated_destination_df

def load_fact_deposit(date):
    
    print('Loading Fact Deposit for ' + date.strftime("%Y-%m-%d"))
    
    deposit_table_name = util.deposit_table_name()
    deposit_schema = util.deposit_schema()
    daily_deposit_df = generate_fact_deposit(date, deposit_table_name, deposit_schema)

    destination_deposit_df_path = 'data-lake/curated/' + deposit_table_name + '.csv'

    if os.path.isfile(destination_deposit_df_path): 
        destination_deposit_df = pd.read_csv(destination_deposit_df_path)
        destination_deposit_df['event_timestamp'] = pd.to_datetime(destination_deposit_df['event_timestamp'], format='mixed')
        final_df = merge_fact_deposit(date, daily_deposit_df, destination_deposit_df)
        final_df.to_csv(destination_deposit_df_path, index=False)
    else:
        # if dim data file doesn't exist yet (first processing)
        daily_deposit_df.to_csv(destination_deposit_df_path, index=False)


def generate_fact_withdrawal(date, withdrawal_table_name, withdrawal_schema):
    withdrawal_file_path = util.data_lake_file_path(withdrawal_table_name, 'trusted', date)

    if not os.path.isfile(withdrawal_file_path): 
        withdrawal_df = util.create_empty_dataframe(withdrawal_schema)
    else:
        withdrawal_df = pd.read_csv(withdrawal_file_path)
    
    withdrawal_df['event_timestamp'] = pd.to_datetime(withdrawal_df['event_timestamp'], format='mixed')
    withdrawal_df['user_id'] = withdrawal_df['user_id'].astype(str)

    return withdrawal_df

def merge_fact_withdrawal(date, source_df, destination_df):
    # Convert the date parameter to a Timestamp for comparison
    load_date = pd.to_datetime(date)

    # Filter out rows in destination_df that match the specified date in event_timestamp, so the process is idempotent
    destination_df = destination_df[
        destination_df['event_timestamp'].dt.date != load_date.date()
    ]
    
    # Append new data from source_df to the updated destination_df
    updated_destination_df = pd.concat([destination_df, source_df], ignore_index=True)

    # Remove any duplicates by 'id', keeping the last occurrence (in case of retries)
    updated_destination_df.drop_duplicates(subset='id', keep='last', inplace=True)

    return updated_destination_df

def load_fact_withdrawal(date):
    
    print('Loading Fact Withdrawal for ' + date.strftime("%Y-%m-%d"))
    
    withdrawal_table_name = util.withdrawal_table_name()
    withdrawal_schema = util.withdrawal_schema()
    daily_withdrawal_df = generate_fact_withdrawal(date, withdrawal_table_name, withdrawal_schema)

    destination_withdrawal_df_path = 'data-lake/curated/' + withdrawal_table_name + '.csv'

    if os.path.isfile(destination_withdrawal_df_path): 
        destination_withdrawal_df = pd.read_csv(destination_withdrawal_df_path)
        destination_withdrawal_df['event_timestamp'] = pd.to_datetime(destination_withdrawal_df['event_timestamp'], format='mixed')
        final_df = merge_fact_withdrawal(date, daily_withdrawal_df, destination_withdrawal_df)
        final_df.to_csv(destination_withdrawal_df_path, index=False)
    else:
        # if dim data file doesn't exist yet (first processing)
        daily_withdrawal_df.to_csv(destination_withdrawal_df_path, index=False)


def generate_fact_user_level(date):

    """
    Returns the most recent level for each user_id and jurisdiction based on the event_timestamp.
    """
    user_level_df = read_user_level_dataframe(date)
    
    # Sort the DataFrame by event_timestamp in descending order
    user_level_df_sorted = user_level_df.sort_values(by='event_timestamp', ascending=False)

    # Drop duplicates to keep only the most recent level for each user_id and jurisdiction
    most_recent_levels = user_level_df_sorted.drop_duplicates(subset=['user_id', 'jurisdiction'])

    return most_recent_levels.reset_index(drop=True)

def merge_fact_user_level(date, user_level_source_df, user_level_destination_df):
    """
    Load user levels into the destination DataFrame after removing records for the specified date.
    """
    # Convert the date string to a Timestamp for comparison
    removal_date = pd.to_datetime(date)

    # Remove records from the destination DataFrame that match the specified date
    user_level_destination_cleaned = user_level_destination_df[
        user_level_destination_df['event_timestamp'].dt.normalize() != removal_date
    ]

    # Merge the cleaned destination DataFrame with the source DataFrame
    updated_destination_df = pd.concat([user_level_destination_cleaned, user_level_source_df])

    return updated_destination_df.reset_index(drop=True)

def load_user_level_fact(date):

    print('Loading Fact User Level for ' + date.strftime("%Y-%m-%d"))
    
    user_level_table_name = util.user_level_table_name()
    user_level_schema = util.user_level_schema()
    user_level_df = generate_fact_user_level(date)

    destination_user_level_df_path = 'data-lake/curated/' + user_level_table_name + '.csv'

    if os.path.isfile(destination_user_level_df_path): 
        destination_user_level_df = pd.read_csv(destination_user_level_df_path)
        destination_user_level_df['event_timestamp'] = pd.to_datetime(destination_user_level_df['event_timestamp'], format='mixed')
        final_df = merge_fact_user_level(date, user_level_df, destination_user_level_df)
        final_df.to_csv(destination_user_level_df_path, index=False)
    else:
        # if dim data file doesn't exist yet (first processing)
        user_level_df.to_csv(destination_user_level_df_path, index=False)

def generate_fact_user_daily_snapshot(date):

    user_df = read_user_id_dataframe(date)
    deposit_df = generate_fact_deposit(date, util.deposit_table_name(), util.deposit_schema())
    withdrawal_df = generate_fact_withdrawal(date, util.withdrawal_table_name(), util.withdrawal_schema())
    event_df = read_event_dataframe(date)
    
    # Convert date parameter to a Timestamp for comparison
    snapshot_date = pd.to_datetime(date)

    # Convert `event_timestamp` to date only for filtering
    deposit_df['event_date'] = deposit_df['event_timestamp'].dt.normalize()
    withdrawal_df['event_date'] = withdrawal_df['event_timestamp'].dt.normalize()
    event_df['event_date'] = event_df['event_timestamp'].dt.normalize()

    # Filter dataframes for the given date
    deposit_on_date = deposit_df[deposit_df['event_date'] == snapshot_date]
    withdrawal_on_date = withdrawal_df[withdrawal_df['event_date'] == snapshot_date]
    logins_on_date = event_df[(event_df['event_date'] == snapshot_date) & (event_df['event_name'].isin(['login', '2falogin', 'login_api']))]

    # Aggregate deposits by user
    deposit_agg = deposit_on_date.groupby('user_id').size().reset_index(name='qty_deposits')
    
    # Aggregate withdrawals by user
    withdrawal_agg = withdrawal_on_date.groupby('user_id').size().reset_index(name='qty_withdrawals')
    
    # Aggregate logins by user
    logins_agg = logins_on_date.groupby('user_id').size().reset_index(name='qty_logins')

    # Mark as active if there is any deposit or withdrawal for the user on that date
    # Use `merge` on user_id to include only those who had deposits or withdrawals
    activity_agg = pd.concat([deposit_agg[['user_id']], withdrawal_agg[['user_id']]]).drop_duplicates()
    activity_agg['is_active'] = True

    # Merge all aggregations with the user_df to ensure all users are included
    user_snapshot = user_df[['user_id']].copy()
    user_snapshot['date'] = snapshot_date

    # Merge the aggregations into the user_snapshot DataFrame
    user_snapshot = user_snapshot.merge(deposit_agg, on='user_id', how='left')
    user_snapshot = user_snapshot.merge(withdrawal_agg, on='user_id', how='left')
    user_snapshot = user_snapshot.merge(logins_agg[['user_id', 'qty_logins']], on='user_id', how='left')
    user_snapshot = user_snapshot.merge(activity_agg[['user_id', 'is_active']], on='user_id', how='left')

    # Fill NaN values for quantity columns with 0 and for is_active with False
    user_snapshot['qty_deposits'].fillna(0, inplace=True)
    user_snapshot['qty_withdrawals'].fillna(0, inplace=True)
    user_snapshot['qty_logins'].fillna(0, inplace=True)
    user_snapshot['is_active'].fillna(False, inplace=True)

    # Convert qty columns to int for consistency with schema
    user_snapshot['qty_deposits'] = user_snapshot['qty_deposits'].astype(int)
    user_snapshot['qty_withdrawals'] = user_snapshot['qty_withdrawals'].astype(int)
    user_snapshot['qty_logins'] = user_snapshot['qty_logins'].astype(int)

    # Filter out users with no deposits, withdrawals, or logins
    # Improvement: this is being done to avoid huge files, but we can keep users with no activity in a day if we change the ETl
    #              to use more performant languages and file formats
    user_snapshot = user_snapshot[
        (user_snapshot['qty_deposits'] > 0) |
        (user_snapshot['qty_withdrawals'] > 0) |
        (user_snapshot['qty_logins'] > 0)
    ]

    return user_snapshot

def merge_fact_user_daily_snapshot(date, source_df, destination_df):
    
    # Convert the date parameter to a Timestamp for comparison
    load_date = pd.to_datetime(date)

    # Filter out rows in destination_df that match the specified date, so the process is idempotent
    destination_df = destination_df[
        destination_df['date'].dt.date != load_date.date()
    ]
    
    # Append new data from source_df to the updated destination_df
    updated_destination_df = pd.concat([destination_df, source_df], ignore_index=True)

    # Remove any duplicates by 'id' and 'date', keeping the last occurrence (in case of retries)
    updated_destination_df.drop_duplicates(subset=['user_id', 'date'], keep='last', inplace=True)

    return updated_destination_df

def load_fact_user_daily_snapshot(date):
    
    print('Loading Fact User Daily Snapshot for ' + date.strftime("%Y-%m-%d"))

    src_user_daily_snapshot_schema_df = generate_fact_user_daily_snapshot(date)

    dest_user_daily_snapshot_df_path = 'data-lake/curated/' + util.fact_user_daily_snapshot_name() + '.csv'

    if os.path.isfile(dest_user_daily_snapshot_df_path): 
        dest_user_daily_snapshot_df = pd.read_csv(dest_user_daily_snapshot_df_path)
        dest_user_daily_snapshot_df['date'] = pd.to_datetime(dest_user_daily_snapshot_df['date'], format='mixed')
        final_df = merge_fact_user_daily_snapshot(date, src_user_daily_snapshot_schema_df, dest_user_daily_snapshot_df)
        final_df.to_csv(dest_user_daily_snapshot_df_path, index=False)
    else:
        # if fact data file doesn't exist yet (first processing)
        src_user_daily_snapshot_schema_df.to_csv(dest_user_daily_snapshot_df_path, index=False)


def generate_fact_daily_stats(date):
    # Convert the date parameter to a Timestamp and normalize to midnight
    snapshot_date = pd.to_datetime(date).normalize()
    
    # Fetch user_level, deposit, and withdrawal dataframes
    user_level_df = read_fact_user_level_dataframe()  # Removed `date` from the function call
    deposit_df = generate_fact_deposit(date, util.deposit_table_name(), util.deposit_schema())
    withdrawal_df = generate_fact_withdrawal(date, util.withdrawal_table_name(), util.withdrawal_schema())

    # Prepare the user_level dataframe to get the most recent level per user, jurisdiction
    user_level_df['event_date'] = user_level_df['event_timestamp'].dt.normalize()
    user_level_on_date = (
        user_level_df[user_level_df['event_date'] <= snapshot_date]
        .sort_values(by=['user_id', 'jurisdiction', 'event_date'], ascending=[True, True, False])
        .drop_duplicates(subset=['user_id', 'jurisdiction'], keep='first')
    )

    # Start with fact_daily_stats as the base, containing unique `level`, `jurisdiction` from `user_level_on_date`
    fact_daily_stats = user_level_on_date[['level', 'jurisdiction']].drop_duplicates()
    fact_daily_stats['date'] = snapshot_date  # Add date column

    # Filter deposit and withdrawal data for the specified date
    deposit_df['event_date'] = deposit_df['event_timestamp'].dt.normalize()
    withdrawal_df['event_date'] = withdrawal_df['event_timestamp'].dt.normalize()
    
    deposits_on_date = deposit_df[deposit_df['event_date'] == snapshot_date]
    withdrawals_on_date = withdrawal_df[withdrawal_df['event_date'] == snapshot_date]

    # Join deposits with user level to ensure proper aggregation
    deposits_joined = deposits_on_date.merge(
        user_level_on_date[['user_id', 'level', 'jurisdiction']],
        on='user_id',
        how='inner'
    )

    # Aggregate deposits by level, jurisdiction, and currency
    deposit_agg = (
        deposits_joined
        .groupby(['level', 'jurisdiction', 'currency'], as_index=False)
        .agg(
            total_deposit_amount=('amount', 'sum'),
            total_distinct_deposit_users=('user_id', 'nunique')
        )
    )

    # Join withdrawals with user level to ensure proper aggregation
    withdrawals_joined = withdrawals_on_date.merge(
        user_level_on_date[['user_id', 'level', 'jurisdiction']],
        on='user_id',
        how='inner'
    )

    # Aggregate withdrawals by level, jurisdiction, and currency
    withdrawal_agg = (
        withdrawals_joined
        .groupby(['level', 'jurisdiction', 'currency'], as_index=False)
        .agg(
            total_withdrawal_amount=('amount', 'sum'),
            total_distinct_withdrawal_users=('user_id', 'nunique')
        )
    )

    # Count distinct active users from deposits and withdrawals
    active_users = pd.concat([
        deposits_joined[['user_id', 'level', 'jurisdiction']],
        withdrawals_joined[['user_id', 'level', 'jurisdiction']]
    ]).drop_duplicates()

    # Aggregate the count of total active users by level and jurisdiction
    total_active_users = (
        active_users
        .groupby(['level', 'jurisdiction'], as_index=False)
        .agg(total_active_users=('user_id', 'nunique'))
    )

    # Create a base DataFrame with all unique combinations of keys from user levels, deposits, and withdrawals
    base_keys = pd.concat([
        deposit_agg[['level', 'jurisdiction', 'currency']],
        withdrawal_agg[['level', 'jurisdiction', 'currency']]
    ]).drop_duplicates()

    # Add date column to the base keys
    base_keys['date'] = snapshot_date

    # Merge deposit and withdrawal aggregations with base_keys to ensure all combinations are included
    fact_daily_stats = fact_daily_stats.merge(base_keys, on=['level', 'jurisdiction', 'date'], how='left')
    fact_daily_stats = fact_daily_stats.merge(deposit_agg, on=['level', 'jurisdiction', 'currency'], how='left')
    fact_daily_stats = fact_daily_stats.merge(withdrawal_agg, on=['level', 'jurisdiction', 'currency'], how='left')
    fact_daily_stats = fact_daily_stats.merge(total_active_users, on=['level', 'jurisdiction'], how='left')

    # Fill NaN values for amounts with 0 and for user counts with 0
    fact_daily_stats.fillna({
        'total_deposit_amount': 0.0,
        'total_withdrawal_amount': 0.0,
        'total_distinct_deposit_users': 0,
        'total_distinct_withdrawal_users': 0,
        'total_active_users': 0
    }, inplace=True)

    # Select the final columns according to the desired output schema
    fact_daily_stats = fact_daily_stats[['date', 'currency', 'level', 'jurisdiction', 
                                         'total_active_users', 'total_distinct_withdrawal_users', 
                                         'total_distinct_deposit_users', 
                                         'total_withdrawal_amount', 'total_deposit_amount']]
    
    # Filter rows to keep only those where at least one measure is greater than zero
    fact_daily_stats = fact_daily_stats[
        (fact_daily_stats['total_active_users'] > 0) |
        (fact_daily_stats['total_distinct_withdrawal_users'] > 0) |
        (fact_daily_stats['total_distinct_deposit_users'] > 0) |
        (fact_daily_stats['total_withdrawal_amount'] > 0) |
        (fact_daily_stats['total_deposit_amount'] > 0)
    ]

    return fact_daily_stats


def merge_fact_daily_stats(date, source_df, destination_df):
    
    # Convert the date parameter to a Timestamp for comparison
    load_date = pd.to_datetime(date)

    # Filter out rows in destination_df that match the specified date, so the process is idempotent
    destination_df = destination_df[
        destination_df['date'].dt.date != load_date.date()
    ]
    
    # Append new data from source_df to the updated destination_df
    updated_destination_df = pd.concat([destination_df, source_df], ignore_index=True)

    # Remove any duplicates by 'id' and 'date', keeping the last occurrence (in case of retries)
    updated_destination_df.drop_duplicates(subset=['date', 'currency', 'level', 'jurisdiction'], keep='last', inplace=True)

    return updated_destination_df

def load_fact_daily_stats(date):
    
    print('Loading Fact Daily Stats for ' + date.strftime("%Y-%m-%d"))

    src_fact_daily_stats_df = generate_fact_daily_stats(date)

    dest_fact_daily_stats_df_path = 'data-lake/curated/' + util.fact_daily_stats_name() + '.csv'

    if os.path.isfile(dest_fact_daily_stats_df_path): 
        dest_fact_daily_snapshot_df = pd.read_csv(dest_fact_daily_stats_df_path)
        dest_fact_daily_snapshot_df['date'] = pd.to_datetime(dest_fact_daily_snapshot_df['date'], format='mixed')
        final_df = merge_fact_daily_stats(date, src_fact_daily_stats_df, dest_fact_daily_snapshot_df)
        final_df.to_csv(dest_fact_daily_stats_df_path, index=False)
    else:
        # if fact data file doesn't exist yet (first processing)
        src_fact_daily_stats_df.to_csv(dest_fact_daily_stats_df_path, index=False)


def load(date):
    """
    This method will load the curated layer (which simulates our DataWarehouse) with all dimensions and fact tables
    Another approach would be to load a real datawarehouse like Google BigQuery or Amazon Redshift, or to create federated
    tables/views on top of theses files. Since the problem statement asks for csv files to be generated, this is being done.

    Every Load method will have two steps, one to generate the new increment for a specific date, and another one to merge the new
    data into the destination (curated layer / simulated DataWarehouse). WA possible improvement would be to write the result of the first
    method (that generates the facts/dimensions) into a temporary file. I haven't done it for the sake of simplicity in this case.

    Default behavior and responsibilities for each step that load dimension and fact tables:
    Generate -> Read all required input data, apply calculations and transformations to match the destination data model
    Merge -> Gets the result from Generate method and the destination table/dataframe and merge it applying the right method
             It takes care of idempotent load and/or updates to dimension tables following specific business rules.
    """
        
    load_dim_user(date)
    load_fact_deposit(date)
    load_fact_withdrawal(date)
    load_fact_user_daily_snapshot(date)
    load_user_level_fact(date)
    load_fact_daily_stats(date)
