with base as (
select
  coalesce(tm.week, lm.week + interval '1 week') as week,

  -- Count weekly Active Users (wau).
  count(distinct tm.user_id) as wau,
  
  -- Count retained users (present in both this week and the last).
  count(distinct case when lm.user_id is not null then tm.user_id else null end) as retained,
  
  -- count new users (whose first_week equals to the current week).
  count(distinct case when tm.first_week = tm.week then tm.user_id else null end) as "new",
  
  -- Count resurrected users (who were not present last week but returned this week).
  count(distinct case when tm.first_week != tm.week and lm.user_id is null then tm.user_id else null end) as resurrected,
  
  -- Count churned users (who were present last week but not this week), making the count negative.
  -1 * count(distinct case when tm.user_id is null then lm.user_id else null end) as churned
  from
    daily_rev tm full 
    outer join daily_rev lm on tm.user_id = lm.user_id
    and tm.week = lm.week + interval '1 week'
  group by 1
)

select 
  base.*, 
  -- preceding_wau is used to calculate week-over-week retention rate.
  lag(wau) over (order by week) as preceding_wau

from base

where week <= date_trunc('week', current_date at time zone 'America/Sao_Paulo')
