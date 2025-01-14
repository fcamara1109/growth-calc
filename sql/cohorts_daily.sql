with 

cohorts as (
  select
    first_dt,
    dt as active_day,
    days_since_first,
    count(distinct user_id) as users,
    sum(inc_amt) as inc_amt
  from
    daily_rev
  group by 1,2,3
),

cohort_sizes as (
  select
    first_dt,
    users,
    inc_amt
  from
    cohorts
  where
    days_since_first = 0
),

-- Generate all possible day combinations with a 90 days limit. This limit can be changed for whatever constant you'd like. Then, in your dashboard limit the number of columns in the pivot table to the same value.
all_days as (
  select
    cs.first_dt,
    (cs.first_dt + (day_number * interval '1 day'))::date as active_day
  from
    cohort_sizes cs,
    generate_series(0, 90) as day_number
),

-- Join cohorts with all possible days
all_cohorts as (
  select
    am.*,
    am.active_day - am.first_dt as days_since_first,
    c.users as users,
    c.inc_amt as inc_amt
  from
    all_days am
    left join cohorts c 
    on am.first_dt = c.first_dt
    and am.active_day = c.active_day
),

-- Calculate cumulative data
cumulative as (
  select
    c1.first_dt,
    c1.active_day,
    c1.days_since_first,
    c1.users,
    c1.inc_amt,
    cs.users as cohort_num_users,
    sum(c2.inc_amt) as cum_amt
  from
    all_cohorts c1
    join all_cohorts c2 on c1.first_dt = c2.first_dt
    and c2.days_since_first <= c1.days_since_first
    join cohort_sizes cs on cs.first_dt = c1.first_dt
  group by 1,2,3,4,5,6
)

select
  first_dt,
  active_day, 
  days_since_first,
  users,
  inc_amt,
  cohort_num_users,
  cum_amt,
  cast(users as float) / cast(cohort_num_users as float) as retention_rate,
  cast(cum_amt as float) / cast(cohort_num_users as float) as ltv
from (
  select 
    *,
    sum(users) over (partition by first_dt, days_since_first) as excess_cut
  from cumulative
) as excess_cut
where excess_cut is not null
and first_dt <= current_date at time zone 'America/Sao_Paulo'