from datetime import datetime, timedelta
import extract_daily_batches as e
import cleanup as c
import load as l

start_date = datetime(2020, 1, 1)
end_date = datetime(2023, 8, 23)
landing_dir = 'data-lake/landing/'
raw_dir = 'data-lake/raw/'


def extract(start_date, end_date, landing_dir, raw_dir):
    # simulate extraction of other tables (containing event_date)
    event_files = ['deposit','event','user_level','withdrawal']
    e.extract_events(event_files, landing_dir, raw_dir)


    # simulate extraction of user_id (no event_date)
    user_id_file_path = landing_dir + 'user_id/user_id_sample_data.csv'
    destination_user_id_directory = raw_dir + 'user_id/' 
    destination_user_id_file_name = 'user_id.csv'
    e.extract_user_id(start_date, end_date, user_id_file_path, destination_user_id_directory, destination_user_id_file_name)


def process_etl():
    
    # for this exercise, we are considering daily batches from 2020-01-01 to 2023-08-23
    start_date = datetime(2020,1,1)
    end_date = datetime(2023,8,23)

    e.extract(start_date, end_date)
    
    current_date = start_date
    while current_date <= end_date:
        c.cleanup(current_date, current_date)
        l.load(current_date)
        current_date = current_date + timedelta(days=1)


process_etl()