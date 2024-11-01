import pandas as pd
import os
import shutil
from datetime import datetime, timedelta
import util



def extract_events(files, landing_dir, raw_dir):
    """
    This method simulates the extraction process, from landing to raw layer, for all files that has a event_timestamp column.
    """
    
    print('Generating daily batches data for event tables (with timestamp)')
    
    input_path = 'data-lake/' + landing_dir + '/{}/{}_sample_data.csv'
    output_path = 'data-lake/' + raw_dir + '/{}/{}'

    for f in files:

        # Load the CSV file
        df = pd.read_csv(input_path.format(f, f))

        # Convert event_timestamp to datetime
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], format='mixed')

        # Group by date
        for date, data in df.groupby(df['event_timestamp'].dt.date):
            # Define directory path for each date
            date_str = date.strftime('%Y-%m-%d')
            dir_path = output_path.format(f, date_str, f)
            os.makedirs(dir_path, exist_ok=True)
            
            # Save each group to a CSV file within the date-specific folder
            data.to_csv(f"{dir_path}/{f}.csv", index=False)
    return True


def extract_user_id(start_date, end_date, landing_dir, raw_dir):
    """
    This method simulates the extraction process, from landing to raw layer, for user_id table.
    Since we don't have a timestamp column, we'll just create several copies of the file in each date folder
    This would be different in a real-world environment and the process would need to be adapted
    """

    print('Generating daily batches data for user_id')

    user_id_table_name = util.user_id_table_name()
    src_file_path = 'data-lake/' + landing_dir + '/' + user_id_table_name+ '/' +  user_id_table_name + '_sample_data.csv'
    destination_directory = 'data-lake/' + raw_dir + '/' + user_id_table_name + '/'
    destination_file_name = user_id_table_name + '.csv'

    current_date = start_date
    while current_date <= end_date:
        # Format the current date as yyyy-mm-dd for the folder name
        folder_name = current_date.strftime('%Y-%m-%d')
        
        # Create the destination folder path
        destination_folder = os.path.join(destination_directory, folder_name)
        os.makedirs(destination_folder, exist_ok=True)
        
        # Define the destination path for the file copy
        #destination_path = os.path.join(destination_folder, os.path.basename(file_path))
        destination_path = os.path.join(destination_folder, destination_file_name)
        
        # Copy the file to the date-specific folder
        shutil.copy(src_file_path, destination_path)
        
        # Move to the next day
        current_date += timedelta(days=1)

def extract(start_date, end_date):
    """
    This process is here only to simulate the daily batches extraction. It will get data from landing layer
    and create the daily increments in raw layer. In an environment closer to real world, we would already start with
    the data from daily increments/batches or something like a CDC.
    """
    
    landing_dir = 'landing'
    raw_dir = 'raw'
    event_files = ['deposit', 'event', 'user_level', 'withdrawal']
    
    extract_events(event_files, landing_dir, raw_dir)
    extract_user_id(start_date, end_date, landing_dir, raw_dir)