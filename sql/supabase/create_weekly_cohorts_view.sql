-- Drop existing view if exists
drop materialized view if exists weekly_cohorts_view;

create materialized view weekly_cohorts_view as
with 

cohorts as (
  select
    first_week,
    week as active_week,
    weeks_since_first,
    count(distinct user_id) as users,
    sum(inc_amt) as inc_amt,
    session_id
  from
    daily_rev_view
  group by 1,2,3,6
),

cohort_sizes as (
  select
    first_week,
    users,
    inc_amt,
    session_id
  from
    cohorts
  where
    weeks_since_first = 0
),

all_weeks as (
  select
    cs.first_week,
    (cs.first_week + (week_number * interval '1 week'))::date as active_week,
    cs.session_id
  from
    cohort_sizes cs,
    generate_series(0, 52) as week_number
),

all_cohorts as (
  select
    am.*,
    (am.active_week - am.first_week) / 7 as weeks_since_first,
    c.users as users,
    c.inc_amt as inc_amt
  from
    all_weeks am
    left join cohorts c 
    on am.first_week = c.first_week
    and am.active_week = c.active_week
    and am.session_id = c.session_id
),

cumulative as (
  select
    c1.first_week,
    c1.active_week,
    c1.weeks_since_first,
    c1.users,
    c1.inc_amt,
    cs.users as cohort_num_users,
    sum(c2.inc_amt) as cum_amt,
    c1.session_id
  from
    all_cohorts c1
    join all_cohorts c2 on c1.first_week = c2.first_week
    and c2.weeks_since_first <= c1.weeks_since_first
    and c1.session_id = c2.session_id
    join cohort_sizes cs on cs.first_week = c1.first_week
    and cs.session_id = c1.session_id
  group by 1,2,3,4,5,6,8
)

select
  first_week,
  active_week, 
  weeks_since_first,
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
    sum(users) over (partition by first_week, weeks_since_first, session_id) as excess_cut
  from cumulative
) as excess_cut
where excess_cut is not null
and first_week <= current_date at time zone 'America/Sao_Paulo'
with no data;

-- Create unique index first (required for concurrent refresh)
create unique index weekly_cohorts_unique_idx 
on weekly_cohorts_view(session_id, first_week, active_week);

-- Create additional indexes
create index idx_weekly_cohorts_first_week 
on weekly_cohorts_view(first_week);

create index idx_weekly_cohorts_active_week 
on weekly_cohorts_view(active_week);

create index idx_weekly_cohorts_session 
on weekly_cohorts_view(session_id); 