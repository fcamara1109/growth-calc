-- Drop existing view if exists
drop materialized view if exists monthly_cohorts_view;

create materialized view monthly_cohorts_view as
with cohorts as (
  select
    first_month,
    month as active_month,
    months_since_first,
    count(distinct user_id) as users,
    sum(inc_amt) as inc_amt,
    session_id
  from
    daily_rev_view
  group by 1,2,3,6
),

cohort_sizes as (
  select
    first_month,
    users,
    inc_amt,
    session_id
  from
    cohorts
  where
    months_since_first = 0
),

all_months as (
  select
    cs.first_month,
    (cs.first_month + (month_number * interval '1 month'))::date as active_month,
    cs.session_id
  from
    cohort_sizes cs,
    generate_series(0, 24) as month_number
),

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
    and am.session_id = c.session_id
),

cumulative as (
  select
    c1.first_month,
    c1.active_month,
    c1.months_since_first,
    c1.users,
    c1.inc_amt,
    cs.users as cohort_num_users,
    sum(c2.inc_amt) as cum_amt,
    c1.session_id
  from
    all_cohorts c1
    join all_cohorts c2 on c1.first_month = c2.first_month
    and c2.months_since_first <= c1.months_since_first
    and c1.session_id = c2.session_id
    join cohort_sizes cs on cs.first_month = c1.first_month
    and cs.session_id = c1.session_id
  group by 1,2,3,4,5,6,8
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
  cast(cum_amt as float) / cast(cohort_num_users as float) as ltv,
  session_id
from (
  select 
    *,
    sum(users) over (partition by first_month, months_since_first, session_id) as excess_cut
  from cumulative
) as excess_cut
where excess_cut is not null
and first_month <= current_date at time zone 'America/Sao_Paulo'
with no data;

-- Create unique index first (required for concurrent refresh)
create unique index monthly_cohorts_unique_idx 
on monthly_cohorts_view(session_id, first_month, active_month);

-- Create additional indexes
create index idx_monthly_cohorts_first_month 
on monthly_cohorts_view(first_month);

create index idx_monthly_cohorts_active_month 
on monthly_cohorts_view(active_month);

create index idx_monthly_cohorts_session 
on monthly_cohorts_view(session_id);

-- Create a unique index on session_id and first_month to improve query performance
create unique index if not exists monthly_cohorts_view_idx 
on monthly_cohorts_view(session_id, first_month, active_month);

-- -- Create a function to refresh the materialized view
-- create or replace function refresh_monthly_cohorts_view()
-- returns trigger as $$
-- begin
--   refresh materialized view concurrently monthly_cohorts_view;
--   return null;
-- end;
-- $$ language plpgsql;

-- -- Create a trigger to refresh the materialized view when the refresh_trigger table is updated
-- drop trigger if exists refresh_monthly_cohorts_view_trigger on refresh_trigger;
-- create trigger refresh_monthly_cohorts_view_trigger
-- after insert on refresh_trigger
-- for each statement
-- execute function refresh_monthly_cohorts_view();