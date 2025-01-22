with base as (
select
  coalesce(tm.dt, lm.dt + interval '1 day') as day,

  -- Count daily Active Users (dau).
  count(distinct tm.user_id) as dau,
  
  -- Count retained users (present in both this day and the last).
  count(distinct case when lm.user_id is not null then tm.user_id else null end) as retained,
  
  -- count new users (whose first_dt equals to the current day).
  count(distinct case when tm.first_dt = tm.dt then tm.user_id else null end) as "new",
  
  -- Count resurrected users (who were not present last day but returned this day).
  count(distinct case when tm.first_dt != tm.dt and lm.user_id is null then tm.user_id else null end) as resurrected,
  
  -- Count churned users (who were present last day but not this day), making the count negative.
  -1 * count(distinct case when tm.user_id is null then lm.user_id else null end) as churned
  from
    daily_rev tm full 
    outer join daily_rev lm on tm.user_id = lm.user_id
    and tm.dt = lm.dt + interval '1 day'
  group by 1
)

select 
  base.*, 
  -- preceding_dau is used to calculate day-over-day retention rate.
  lag(dau) over (order by day) as preceding_dau

from base

where day <= date_trunc('day', current_date at time zone 'America/Sao_Paulo')
