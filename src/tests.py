import unittest
import pandas as pd
from pandas.testing import assert_frame_equal

import load as l

class TestMergeDimUser(unittest.TestCase):
    
    def test_new_user_insertion(self):
        source_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'last_login': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-05')]
        })
        destination_df = pd.DataFrame({
            'user_id': ['user1'],
            'last_login': [pd.Timestamp('2022-12-31')]
        })
        
        expected_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'last_login': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-05')]
        })

        result_df = l.merge_dim_user(source_df, destination_df)
        assert_frame_equal(result_df.sort_values(by='user_id').reset_index(drop=True), 
                           expected_df.sort_values(by='user_id').reset_index(drop=True))

    def test_last_login_update(self):
        source_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'last_login': [pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-04')]
        })
        destination_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'last_login': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-03')]
        })
        
        expected_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'last_login': [pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-04')]
        })

        result_df = l.merge_dim_user(source_df, destination_df)
        assert_frame_equal(result_df.sort_values(by='user_id').reset_index(drop=True), 
                           expected_df.sort_values(by='user_id').reset_index(drop=True))

    def test_no_update_needed(self):
        source_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'last_login': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-02')]
        })
        destination_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'last_login': [pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-03')]
        })
        
        expected_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'last_login': [pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-03')]
        })

        result_df = l.merge_dim_user(source_df, destination_df)
        assert_frame_equal(result_df.sort_values(by='user_id').reset_index(drop=True), 
                           expected_df.sort_values(by='user_id').reset_index(drop=True))

class TestMergeFactDeposit(unittest.TestCase):

    def test_remove_specified_date_records(self):
        source_df = pd.DataFrame({
            'id': [4, 5],
            'event_timestamp': [pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')],
            'user_id': ['user1', 'user2'],
            'amount': [100.0, 200.0],
            'currency': ['USD', 'USD'],
            'tx_status': ['completed', 'pending']
        })

        destination_df = pd.DataFrame({
            'id': [1, 2, 3],
            'event_timestamp': [pd.Timestamp('2023-01-02 07:00:00'), pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-02 11:00:00')],
            'user_id': ['user3', 'user4', 'user5'],
            'amount': [50.0, 150.0, 250.0],
            'currency': ['USD', 'USD', 'USD'],
            'tx_status': ['completed', 'completed', 'pending']
        })

        expected_df = pd.DataFrame({
            'id': [2, 4, 5],
            'event_timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')],
            'user_id': ['user4', 'user1', 'user2'],
            'amount': [150.0, 100.0, 200.0],
            'currency': ['USD', 'USD', 'USD'],
            'tx_status': ['completed', 'completed', 'pending']
        })

        result_df = l.merge_fact_deposit('2023-01-02', source_df, destination_df)
        assert_frame_equal(result_df.sort_values(by='id').reset_index(drop=True), 
                           expected_df.sort_values(by='id').reset_index(drop=True))

    def test_append_new_records(self):
        source_df = pd.DataFrame({
            'id': [6, 7],
            'event_timestamp': [pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')],
            'user_id': ['user5', 'user6'],
            'amount': [300.0, 400.0],
            'currency': ['USD', 'USD'],
            'tx_status': ['completed', 'completed']
        })

        destination_df = pd.DataFrame({
            'id': [1, 2],
            'event_timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-03 11:00:00')],
            'user_id': ['user1', 'user2'],
            'amount': [100.0, 200.0],
            'currency': ['USD', 'USD'],
            'tx_status': ['completed', 'pending']
        })

        expected_df = pd.DataFrame({
            'id': [1, 2, 6, 7],
            'event_timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-03 11:00:00'), pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')],
            'user_id': ['user1', 'user2', 'user5', 'user6'],
            'amount': [100.0, 200.0, 300.0, 400.0],
            'currency': ['USD', 'USD', 'USD', 'USD'],
            'tx_status': ['completed', 'pending', 'completed', 'completed']
        })

        result_df = l.merge_fact_deposit('2023-01-02', source_df, destination_df)
        assert_frame_equal(result_df.sort_values(by='id').reset_index(drop=True), 
                           expected_df.sort_values(by='id').reset_index(drop=True))


class TestMergeFactWithdrawal(unittest.TestCase):

    def test_remove_specified_date_records(self):
        source_df = pd.DataFrame({
            'id': [4, 5],
            'event_timestamp': [pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')],
            'user_id': ['user1', 'user2'],
            'amount': [100.0, 200.0],
            'interface': ['mobile', 'web'],
            'currency': ['USD', 'USD'],
            'tx_status': ['completed', 'pending']
        })

        destination_df = pd.DataFrame({
            'id': [1, 2, 3],
            'event_timestamp': [pd.Timestamp('2023-01-02 07:00:00'), pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-02 11:00:00')],
            'user_id': ['user3', 'user4', 'user5'],
            'amount': [50.0, 150.0, 250.0],
            'interface': ['web', 'mobile', 'web'],
            'currency': ['USD', 'USD', 'USD'],
            'tx_status': ['completed', 'completed', 'pending']
        })

        expected_df = pd.DataFrame({
            'id': [2, 4, 5],
            'event_timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')],
            'user_id': ['user4', 'user1', 'user2'],
            'amount': [150.0, 100.0, 200.0],
            'interface': ['mobile', 'mobile', 'web'],
            'currency': ['USD', 'USD', 'USD'],
            'tx_status': ['completed', 'completed', 'pending']
        })

        result_df = l.merge_fact_withdrawal('2023-01-02', source_df, destination_df)
        assert_frame_equal(result_df.sort_values(by='id').reset_index(drop=True), 
                           expected_df.sort_values(by='id').reset_index(drop=True))

    def test_append_new_records(self):
        source_df = pd.DataFrame({
            'id': [6, 7],
            'event_timestamp': [pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')],
            'user_id': ['user5', 'user6'],
            'amount': [300.0, 400.0],
            'interface': ['web', 'mobile'],
            'currency': ['USD', 'USD'],
            'tx_status': ['completed', 'completed']
        })

        destination_df = pd.DataFrame({
            'id': [1, 2],
            'event_timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-03 11:00:00')],
            'user_id': ['user1', 'user2'],
            'amount': [100.0, 200.0],
            'interface': ['web', 'mobile'],
            'currency': ['USD', 'USD'],
            'tx_status': ['completed', 'pending']
        })

        expected_df = pd.DataFrame({
            'id': [1, 2, 6, 7],
            'event_timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-03 11:00:00'), pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')],
            'user_id': ['user1', 'user2', 'user5', 'user6'],
            'amount': [100.0, 200.0, 300.0, 400.0],
            'interface': ['web', 'mobile', 'web', 'mobile'],
            'currency': ['USD', 'USD', 'USD', 'USD'],
            'tx_status': ['completed', 'pending', 'completed', 'completed']
        })

        result_df = l.merge_fact_withdrawal('2023-01-02', source_df, destination_df)
        assert_frame_equal(result_df.sort_values(by='id').reset_index(drop=True), 
                           expected_df.sort_values(by='id').reset_index(drop=True))


class TestMergeFactUserLevel(unittest.TestCase):

    def test_remove_specified_date_records(self):
        user_level_source_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'jurisdiction': ['US', 'CA'],
            'level': [3, 4],
            'event_timestamp': [pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')]
        })

        user_level_destination_df = pd.DataFrame({
            'user_id': ['user1', 'user3', 'user4'],
            'jurisdiction': ['US', 'US', 'CA'],
            'level': [1, 2, 3],
            'event_timestamp': [pd.Timestamp('2023-01-02 07:00:00'), pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-02 11:00:00')]
        })

        expected_df = pd.DataFrame({
            'user_id': ['user3', 'user1', 'user2'],
            'jurisdiction': ['US', 'US', 'CA'],
            'level': [2, 3, 4],
            'event_timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')]
        })

        result_df = l.merge_fact_user_level('2023-01-02', user_level_source_df, user_level_destination_df)
        assert_frame_equal(result_df.sort_values(by=['user_id', 'jurisdiction']).reset_index(drop=True),
                           expected_df.sort_values(by=['user_id', 'jurisdiction']).reset_index(drop=True))

    def test_append_new_records(self):
        user_level_source_df = pd.DataFrame({
            'user_id': ['user5', 'user6'],
            'jurisdiction': ['UK', 'FR'],
            'level': [5, 6],
            'event_timestamp': [pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')]
        })

        user_level_destination_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'jurisdiction': ['US', 'CA'],
            'level': [1, 2],
            'event_timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-03 11:00:00')]
        })

        expected_df = pd.DataFrame({
            'user_id': ['user1', 'user2', 'user5', 'user6'],
            'jurisdiction': ['US', 'CA', 'UK', 'FR'],
            'level': [1, 2, 5, 6],
            'event_timestamp': [pd.Timestamp('2023-01-01 10:00:00'), pd.Timestamp('2023-01-03 11:00:00'), pd.Timestamp('2023-01-02 08:00:00'), pd.Timestamp('2023-01-02 09:00:00')]
        })

        result_df = l.merge_fact_user_level('2023-01-02', user_level_source_df, user_level_destination_df)
        assert_frame_equal(result_df.sort_values(by=['user_id', 'jurisdiction']).reset_index(drop=True),
                           expected_df.sort_values(by=['user_id', 'jurisdiction']).reset_index(drop=True))


class TestMergeFactUserDailySnapshot(unittest.TestCase):

    def test_remove_specified_date_records(self):
        source_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'date': [pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-02')],
            'qty_deposits': [2, 1],
            'qty_withdrawals': [1, 0],
            'qty_logins': [3, 1],
            'is_active': [True, True]
        })

        destination_df = pd.DataFrame({
            'user_id': ['user1', 'user3', 'user4'],
            'date': [pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-02')],
            'qty_deposits': [1, 3, 4],
            'qty_withdrawals': [1, 1, 0],
            'qty_logins': [2, 1, 2],
            'is_active': [True, False, True]
        })

        expected_df = pd.DataFrame({
            'user_id': ['user3', 'user1', 'user2'],
            'date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-02')],
            'qty_deposits': [3, 2, 1],
            'qty_withdrawals': [1, 1, 0],
            'qty_logins': [1, 3, 1],
            'is_active': [False, True, True]
        })

        result_df = l.merge_fact_user_daily_snapshot('2023-01-02', source_df, destination_df)
        assert_frame_equal(result_df.sort_values(by=['user_id', 'date']).reset_index(drop=True),
                           expected_df.sort_values(by=['user_id', 'date']).reset_index(drop=True))

    def test_append_new_records(self):
        source_df = pd.DataFrame({
            'user_id': ['user5', 'user6'],
            'date': [pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-02')],
            'qty_deposits': [5, 6],
            'qty_withdrawals': [2, 3],
            'qty_logins': [4, 5],
            'is_active': [True, True]
        })

        destination_df = pd.DataFrame({
            'user_id': ['user1', 'user2'],
            'date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-03')],
            'qty_deposits': [1, 2],
            'qty_withdrawals': [1, 0],
            'qty_logins': [2, 3],
            'is_active': [True, True]
        })

        expected_df = pd.DataFrame({
            'user_id': ['user1', 'user2', 'user5', 'user6'],
            'date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-03'), pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-02')],
            'qty_deposits': [1, 2, 5, 6],
            'qty_withdrawals': [1, 0, 2, 3],
            'qty_logins': [2, 3, 4, 5],
            'is_active': [True, True, True, True]
        })

        result_df = l.merge_fact_user_daily_snapshot('2023-01-02', source_df, destination_df)
        assert_frame_equal(result_df.sort_values(by=['user_id', 'date']).reset_index(drop=True),
                           expected_df.sort_values(by=['user_id', 'date']).reset_index(drop=True))

class TestMergeFactDailyStats(unittest.TestCase):

    def test_remove_specified_date_records(self):
        source_df = pd.DataFrame({
            'date': [pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-02')],
            'currency': ['USD', 'EUR'],
            'level': [1, 2],
            'jurisdiction': ['US', 'EU'],
            'total_active_users': [100, 200],
            'total_distinct_withdrawal_users': [10, 20],
            'total_distinct_deposit_users': [5, 15],
            'total_withdrawal_amount': [1000.0, 2000.0],
            'total_deposit_amount': [500.0, 1500.0]
        })

        destination_df = pd.DataFrame({
            'date': [pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-02')],
            'currency': ['USD', 'USD', 'EUR'],
            'level': [1, 2, 2],
            'jurisdiction': ['US', 'US', 'EU'],
            'total_active_users': [50, 150, 180],
            'total_distinct_withdrawal_users': [5, 15, 18],
            'total_distinct_deposit_users': [2, 10, 12],
            'total_withdrawal_amount': [500.0, 1500.0, 1800.0],
            'total_deposit_amount': [250.0, 1200.0, 1400.0]
        })

        expected_df = pd.DataFrame({
            'date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-02')],
            'currency': ['USD', 'USD', 'EUR'],
            'level': [2, 1, 2],
            'jurisdiction': ['US', 'US', 'EU'],
            'total_active_users': [150, 100, 200],
            'total_distinct_withdrawal_users': [15, 10, 20],
            'total_distinct_deposit_users': [10, 5, 15],
            'total_withdrawal_amount': [1500.0, 1000.0, 2000.0],
            'total_deposit_amount': [1200.0, 500.0, 1500.0]
        })

        result_df = l.merge_fact_daily_stats('2023-01-02', source_df, destination_df)
        assert_frame_equal(result_df.sort_values(by=['date', 'currency', 'level', 'jurisdiction']).reset_index(drop=True),
                           expected_df.sort_values(by=['date', 'currency', 'level', 'jurisdiction']).reset_index(drop=True))

    def test_append_new_records(self):
        source_df = pd.DataFrame({
            'date': [pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-02')],
            'currency': ['USD', 'EUR'],
            'level': [1, 2],
            'jurisdiction': ['US', 'EU'],
            'total_active_users': [100, 200],
            'total_distinct_withdrawal_users': [10, 20],
            'total_distinct_deposit_users': [5, 15],
            'total_withdrawal_amount': [1000.0, 2000.0],
            'total_deposit_amount': [500.0, 1500.0]
        })

        destination_df = pd.DataFrame({
            'date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-03')],
            'currency': ['USD', 'USD'],
            'level': [1, 2],
            'jurisdiction': ['US', 'US'],
            'total_active_users': [50, 150],
            'total_distinct_withdrawal_users': [5, 15],
            'total_distinct_deposit_users': [2, 10],
            'total_withdrawal_amount': [500.0, 1500.0],
            'total_deposit_amount': [250.0, 1200.0]
        })

        expected_df = pd.DataFrame({
            'date': [pd.Timestamp('2023-01-01'), pd.Timestamp('2023-01-03'), pd.Timestamp('2023-01-02'), pd.Timestamp('2023-01-02')],
            'currency': ['USD', 'USD', 'USD', 'EUR'],
            'level': [1, 2, 1, 2],
            'jurisdiction': ['US', 'US', 'US', 'EU'],
            'total_active_users': [50, 150, 100, 200],
            'total_distinct_withdrawal_users': [5, 15, 10, 20],
            'total_distinct_deposit_users': [2, 10, 5, 15],
            'total_withdrawal_amount': [500.0, 1500.0, 1000.0, 2000.0],
            'total_deposit_amount': [250.0, 1200.0, 500.0, 1500.0]
        })

        result_df = l.merge_fact_daily_stats('2023-01-02', source_df, destination_df)
        assert_frame_equal(result_df.sort_values(by=['date', 'currency', 'level', 'jurisdiction']).reset_index(drop=True),
                           expected_df.sort_values(by=['date', 'currency', 'level', 'jurisdiction']).reset_index(drop=True))


if __name__ == '__main__':
    unittest.main()
