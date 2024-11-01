import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import util

def is_valid_timestamp(value):
    """
    Check if a given string can be parsed as a timestamp.
    """
    try:
        pd.to_datetime(value)
        return True
    except (ValueError, TypeError):
        return False

def validate_row(row, schema):
    """
    Validate a row against the provided schema.
    Each field's type is checked based on schema, and strings that are supposed to be timestamps are verified.
    """
    for key, value_type in schema.items():
        if key in row:
            # If the expected type is a timestamp string, validate it
            if value_type == pd.Timestamp and isinstance(row[key], str):
                if not is_valid_timestamp(row[key]):
                    return False
            # Otherwise, check if the type matches directly
            elif value_type != pd.Timestamp and not isinstance(row[key], value_type):
                return False
    return True

def validate_dataframe_schema(df, schema):
    """
    Validate the entire DataFrame against the provided schema.
    Each column is validated based on schema, using vectorized operations.
    """
    # Start with all rows as valid
    is_valid = pd.Series([True] * len(df), index=df.index)  # Ensure alignment with df's index

    for col, expected_type in schema.items():
        if col in df.columns:
            if expected_type == pd.Timestamp:
                # Convert column to datetime, setting invalid formats to NaT
                valid_timestamp = pd.to_datetime(df[col], errors='coerce').notna()
                is_valid &= valid_timestamp  # Mark invalid rows as False
            else:
                # Check if column is of the correct type
                valid_type = df[col].apply(lambda x: isinstance(x, expected_type))
                is_valid &= valid_type

    valid_df = df[is_valid].reset_index(drop=True)
    return valid_df


def normalize_timestamp_column(df, schema):
    for key, value_type in schema.items():
        if value_type == pd.Timestamp:
            # Convert column to datetime, errors='coerce' will set invalid dates to NaT
            df[key] = pd.to_datetime(df[key], errors='coerce')

            # Drop timezone information and milliseconds by formatting datetime to desired string format
            df[key] = df[key].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df

def discard_empty_primary_key_rows(df, primary_keys):
    # Drop rows where any of the primary key columns are null or empty
    mask = df[primary_keys].isnull().any(axis=1) | (df[primary_keys] == "").any(axis=1)
    df_cleaned = df[~mask]
    return df_cleaned

def cleanup_and_save(input_path, output_path, primary_keys, schema):
    """
    Load a CSV from input_path, clean it by removing duplicates and rows that
    don't match the schema, normalize timestamp formats and save it to output_path.
    """
    # todo: improve handling of tables with non-existing data for a specific date
    if not os.path.isfile(input_path): 
        return True
        
    try:
        df = pd.read_csv(input_path)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)

    # Drop duplicate rows based on primary keys
    df.drop_duplicates(subset=primary_keys, inplace=True, keep='last')

    # Remove rows with empty or null primary keys
    df = discard_empty_primary_key_rows(df, primary_keys)

    # Filter rows that match schema
    cleaned_df = validate_dataframe_schema(df, schema)

    # Normalize timestamp columns
    cleaned_df = normalize_timestamp_column(cleaned_df, schema)

    # Save the cleaned data to output_path
    try:
        output_dir = os.path.dirname(output_path)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        cleaned_df.to_csv(output_path, index=False)
        print(f"Cleaned data saved to {output_path}")
    except Exception as e:
        print(f"Error saving CSV file: {e}")
        sys.exit(1)


def clean(start_date, end_date, raw_dir, trusted_dir, primary_keys, schema, table_name):
    """
    Handles the input and output paths with dates and iterates over a range of dates,
    calling cleanup_and_save function to remove duplicates and enforce schema.
    """

    input_path = raw_dir + '{}/{}/{}.csv'
    output_path = trusted_dir + '{}/{}/{}.csv'

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        input_path_date = input_path.format(table_name, date_str, table_name)
        output_path_date = output_path.format(table_name, date_str, table_name)
        cleanup_and_save(input_path_date, output_path_date, primary_keys, schema)
        
        # Move to the next day
        current_date += timedelta(days=1)


def cleanup(start_date, end_date):
    """
    This method will trigger cleanup_and_save method for all tables in raw layer, so they are cleaned and saved in trusted layer
    """

    raw_dir = 'data-lake/raw/'
    trusted_dir = 'data-lake/trusted/'

    # user_level
    primary_keys = util.user_level_pk()
    schema = util.user_level_schema()
    table_name = util.user_level_table_name()
    print('Starting cleanup of user_level data')
    clean(start_date, end_date, raw_dir, trusted_dir, primary_keys, schema, table_name)

    # withdrawal
    primary_keys = util.withdrawal_pk()
    schema = util.withdrawal_schema()
    table_name = util.withdrawal_table_name()
    print('Starting cleanup of withdrawal data')
    clean(start_date, end_date, raw_dir, trusted_dir, primary_keys, schema, table_name)

    # user_id
    primary_keys = util.user_id_pk()
    schema = util.user_id_schema()
    table_name = util.user_id_table_name()
    print('Starting cleanup of user_id data')
    clean(start_date, end_date, raw_dir, trusted_dir, primary_keys, schema, table_name)

    # event
    primary_keys = util.event_pk()
    schema = util.event_schema()
    table_name = util.event_table_name()
    print('Starting cleanup of event data')
    clean(start_date, end_date, raw_dir, trusted_dir, primary_keys, schema, table_name)

    # deposit
    primary_keys = util.deposit_pk()
    schema = util.deposit_schema()
    table_name = util.deposit_table_name()
    print('Starting cleanup of deposit data')
    clean(start_date, end_date, raw_dir, trusted_dir, primary_keys, schema, table_name)