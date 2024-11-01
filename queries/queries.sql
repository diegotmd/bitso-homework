-- How many users were active on a given day
-- Option 1: specific day
 select count(*) as total_active_users
   from user_daily_snapshots u
 where u.date = '2023-01-01'
 
 -- Option 2: all dates
 select u.date, count(*) as total_active_users
   from user_daily_snapshots u
 group by u.date
 order by u.date asc
 
 -- Identify users haven't made a deposit
 select u.*
   from dim_user u
  where not exists (select 1
                      from deposit d
                     where d.user_id = u.user_id)

-- Latest user level for each user within each jurisdiction
select user_id, jurisdiction, current_level
  from dim_user_jurisdiction

-- Identify on a given day which users have made more than 5 deposits historically
-- Option 1: if the idea is to identify users who have made more than 5 deposits until that day:
select user_id, count(*) as qty_deposity
  from deposit
 where event_timestamp <= '2023-01-01'
 group by user_id
 having count(*) > 5

-- Option 2: if the idea is to identify users who have made more than 5 deposits in a single day, for all history:
select user_id, date
  from user_daily_snapshots 
 where qty_deposits >= 5

-- When was the last time a user made a login
select user_id, last_login
  from dim_user
 where user_id = '' -- add user id here or remove where to get for all users

-- How many times a user has made a login between two dates
select user_id, sum(qty_logins) qty_logins
  from user_daily_snapshots
 where date between '2023-01-01' and '2023-06-30'
group by user_id

-- Number of unique currencies deposited on a given day
-- Option 1
select count(distinct currency) as qty_currency
  from daily_stats
 where date = '2023-01-01'
   and total_deposit_amount > 0

-- Option 2
select count(distinct currency) as qty_currency
  from deposit
 where event_timestamp = '2023-01-01'

-- Number of unique currencies withdrew on a given day
-- Option 1
select count(distinct currency) as qty_currency
  from daily_stats
 where date = '2023-01-01'
   and total_withdrawal_amount > 0

-- Option 2
select count(distinct currency) as qty_currency
  from withdrawal
 where event_timestamp = '2023-01-01'
