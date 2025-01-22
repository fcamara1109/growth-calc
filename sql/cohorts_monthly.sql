with 

cohorts as (
  select
    first_month,
    month as active_month,
    months_since_first,
    count(distinct user_id) as users,
    sum(inc_amt) as inc_amt
  from
    daily_rev
  group by 1,2,3
),

cohort_sizes as (
  select
    first_month,
    users,
    inc_amt
  from
    cohorts
  where
    months_since_first = 0
),

-- Generate all possible month combinations with a 24 months limit. This limit can be changed for whatever constant you'd like. Then, in your dashboard limit the number of columns in the pivot table to the same value.
all_months as (
  select
    cs.first_month,
    (cs.first_month + (month_number * interval '1 month'))::date as active_month
  from
    cohort_sizes cs,
    generate_series(0, 24) as month_number
),

-- Join cohorts with all possible months
all_cohorts as (
  select
    am.*,
    extract(year from age(am.active_month, am.first_month)) * 12 + extract(month from age(am.active_month, am.first_month)) as months_since_first,
    c.users as users,
    c.inc_amt as inc_amt
  from
    all_months am
    left join cohorts c 
    on am.first_month = c.first_month
    and am.active_month = c.active_month
),

-- Calculate cumulative data
cumulative as (
  select
    c1.first_month,
    c1.active_month,
    c1.months_since_first,
    c1.users,
    c1.inc_amt,
    cs.users as cohort_num_users,
    sum(c2.inc_amt) as cum_amt
  from
    all_cohorts c1
    join all_cohorts c2 on c1.first_month = c2.first_month
    and c2.months_since_first <= c1.months_since_first
    join cohort_sizes cs on cs.first_month = c1.first_month
  group by 1,2,3,4,5,6
)

select
  first_month,
  active_month, 
  months_since_first,
  users,
  inc_amt,
  cohort_num_users,
  cum_amt,
  cast(users as float) / cast(cohort_num_users as float) as retention_rate,
  cast(cum_amt as float) / cast(cohort_num_users as float) as ltv
from (
  select 
    *,
    sum(users) over (partition by first_month, months_since_first) as excess_cut
  from cumulative
) as excess_cut
where excess_cut is not null
and first_month <= current_date at time zone 'America/Sao_Paulo'