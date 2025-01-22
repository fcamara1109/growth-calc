with 

cohorts as (
  select
    first_week,
    week as active_week,
    weeks_since_first,
    count(distinct user_id) as users,
    sum(inc_amt) as inc_amt
  from
    daily_rev
  group by 1,2,3
),

cohort_sizes as (
  select
    first_week,
    users,
    inc_amt
  from
    cohorts
  where
    weeks_since_first = 0
),

-- Generate all possible week combinations with a 52 weeks limit. This limit can be changed for whatever constant you'd like. Then, in your dashboard limit the number of columns in the pivot table to the same value.
all_weeks as (
  select
    cs.first_week,
    (cs.first_week + (week_number * interval '1 week'))::date as active_week
  from
    cohort_sizes cs,
    generate_series(0, 52) as week_number
),

-- Join cohorts with all possible weeks
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
),

-- Calculate cumulative data
cumulative as (
  select
    c1.first_week,
    c1.active_week,
    c1.weeks_since_first,
    c1.users,
    c1.inc_amt,
    cs.users as cohort_num_users,
    sum(c2.inc_amt) as cum_amt
  from
    all_cohorts c1
    join all_cohorts c2 on c1.first_week = c2.first_week
    and c2.weeks_since_first <= c1.weeks_since_first
    join cohort_sizes cs on cs.first_week = c1.first_week
  group by 1,2,3,4,5,6
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
  cast(cum_amt as float) / cast(cohort_num_users as float) as ltv
from (
  select 
    *,
    sum(users) over (partition by first_week, weeks_since_first) as excess_cut
  from cumulative
) as excess_cut
where excess_cut is not null
and first_week <= current_date at time zone 'America/Sao_Paulo'