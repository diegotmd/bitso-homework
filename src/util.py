import pandas as pd
from datetime import datetime


def data_lake_file_path(table_name, layer, date):
    date_str = date.strftime('%Y-%m-%d')
    return 'data-lake/' + layer + '/' + table_name + '/' + date_str + '/' + table_name + '.csv'

def load_csv_to_dataframe(table_name, layer, date, schema):
    path = data_lake_file_path(table_name, layer, date)

def create_empty_dataframe(schema):
    # Initialize an empty DataFrame with the specified column names and types
    schema = {col: ('datetime64[ns]' if col_type == pd.Timestamp else col_type) for col, col_type in schema.items()}
    df = pd.DataFrame({col: pd.Series(dtype=col_type) for col, col_type in schema.items()})
    return df

def user_level_pk():
    # pk is being assumed in this case, we would need more information to properly handle duplicates here
    # we saw there are duplicates in the data, for the same user, jurisdiction and timestamp, there are different levels
    # we would need more information to handle this, so we are assuming we can just arbritarily drop one of the rows
    # so, we are keeping the last valid row in the drop_duplicates
    # improvement: implement a business rule to keep the best row, or review the PK definition
    return ['user_id', 'jurisdiction', 'event_timestamp'] 

def user_level_table_name():
    return 'user_level'

def user_level_schema():
    schema = {
        'user_id': str,
        'jurisdiction': str, 
        'level': int,
        'event_timestamp': pd.Timestamp
    }
    return schema

def withdrawal_pk():
    return ['id']

def withdrawal_table_name():
    return 'withdrawal'

def withdrawal_schema():
    schema = {
        'id': int,
        'event_timestamp': pd.Timestamp, 
        'user_id': str,
        'amount': float,
        'interface': str,
        'currency': str,
        'tx_status': str
    }
    return schema

def user_id_pk():
    return ['user_id']

def user_id_table_name():
    return 'user_id'

def user_id_schema():
    schema = {
        'user_id': str
    }
    return schema;

def event_pk():
    return ['id']

def event_table_name():
    return 'event'

def event_schema():
    schema = {
        'id': int,
        'event_timestamp': pd.Timestamp, 
        'user_id': str,
        'event_name': str
    }
    return schema

def deposit_pk():
    return ['id']

def deposit_table_name():
    return 'deposit'

def deposit_schema():
    schema = {
        'id': int,
        'event_timestamp': pd.Timestamp, 
        'user_id': str,
        'amount': float,
        'currency': str,
        'tx_status': str
    }
    return schema

def dim_user_table_name():
    return 'dim_user'

def fact_user_daily_snapshot_name():
    return 'user_daily_snapshot'

def fact_user_daily_snapshot_schema():
    schema = {
        'user_id': str,
        'date': pd.Timestamp, 
        'qty_deposits': int,
        'qty_withdrawals': int,
        'qty_logins': int,
        'is_active': bool
    }
    return schema

def fact_daily_stats_name():
    return 'daily_stats'

def fact_daily_stats_schema():
    schema = {
        'date': pd.Timestamp, 
        'currency': str,
        'level': int,
        'jurisdiction': str,
        'total_active_users': int,
        'total_distinct_withdrawal_users': int,
        'total_distinct_deposit_users': int,
        'total_withdrawal_amount': float,
        'total_deposit_amount': float,
    }
    return schema

