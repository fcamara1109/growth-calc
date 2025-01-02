with base as (
  select
    user_id,
    transaction_date as dt,
    (date_trunc('week', transaction_date + interval '1 day') - interval '1 day')::date as week,
    date_trunc('month', transaction_date)::date as month,
    sum(revenue) as inc_amt,
    min(transaction_date) over (partition by user_id) as first_dt,
    (date_trunc('week', min(transaction_date) over (partition by user_id) + interval '1 day') - interval '1 day')::date as first_week,
    date_trunc('month', min(transaction_date) over (partition by user_id))::date as first_month
  from revenue_data
  group by 1, 2, 3, 4
)

-- calculate time since first purchase
select 
  base.*,
  dt - base.first_dt as days_since_first,
  (week - base.first_week) / 7 as weeks_since_first,
  extract(year from age(month, base.first_month)) * 12 + extract(month from age(month, base.first_month)) as months_since_first
from base