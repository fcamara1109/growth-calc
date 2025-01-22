-- Drop existing view if exists
drop materialized view if exists daily_cohorts_view;

-- Create materialized view for daily cohorts
create materialized view daily_cohorts_view as
with 

cohorts as (
  select
    first_dt,
    dt as active_day,
    days_since_first,
    count(distinct user_id) as users,
    sum(inc_amt) as inc_amt,
    session_id
  from
    daily_rev_view
  group by 1,2,3,6
),

cohort_sizes as (
  select
    first_dt,
    users,
    inc_amt,
    session_id
  from
    cohorts
  where
    days_since_first = 0
),

all_days as (
  select
    cs.first_dt,
    (cs.first_dt + (day_number * interval '1 day'))::date as active_day,
    cs.session_id
  from
    cohort_sizes cs,
    generate_series(0, 90) as day_number
),

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
    and am.session_id = c.session_id
),

cumulative as (
  select
    c1.first_dt,
    c1.active_day,
    c1.days_since_first,
    c1.users,
    c1.inc_amt,
    cs.users as cohort_num_users,
    sum(c2.inc_amt) as cum_amt,
    c1.session_id
  from
    all_cohorts c1
    join all_cohorts c2 on c1.first_dt = c2.first_dt
    and c2.days_since_first <= c1.days_since_first
    and c1.session_id = c2.session_id
    join cohort_sizes cs on cs.first_dt = c1.first_dt
    and cs.session_id = c1.session_id
  group by 1,2,3,4,5,6,8
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
  cast(cum_amt as float) / cast(cohort_num_users as float) as ltv,
  session_id
from (
  select 
    *,
    sum(users) over (partition by first_dt, days_since_first, session_id) as excess_cut
  from cumulative
) as excess_cut
where excess_cut is not null
and first_dt <= current_date at time zone 'America/Sao_Paulo'
with no data;

-- Create unique index first (required for concurrent refresh)
create unique index daily_cohorts_unique_idx 
on daily_cohorts_view(session_id, first_dt, active_day);

-- Create additional indexes
create index idx_daily_cohorts_first_dt 
on daily_cohorts_view(first_dt);

create index idx_daily_cohorts_active_day 
on daily_cohorts_view(active_day);

create index idx_daily_cohorts_session 
on daily_cohorts_view(session_id); 