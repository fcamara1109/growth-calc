with dayly_rev as (
  select
    user_id,
    dt,
    first_dt,
    days_since_first,
    sum(inc_amt) as inc_amt
  from daily_rev
  group by user_id, dt, first_dt, days_since_first
),

base as (
  select
    coalesce(tm.dt, lm.dt + interval '1 day') as day,

    -- Total revenue for the day.
    sum(tm.inc_amt) as rev, 

    -- Revenue from users who were active last day and remained active.
    sum(
      case 
        when tm.user_id is not null and lm.user_id is not null and tm.inc_amt >= lm.inc_amt then lm.inc_amt 
        when tm.user_id is not null and lm.user_id is not null and tm.inc_amt < lm.inc_amt then tm.inc_amt 
        else 0 
      end
    ) as retained, 
    
    -- Revenue from new users who were active for the first time this day.
    sum(
      case 
        when tm.first_dt = tm.dt then tm.inc_amt 
        else 0 
      end
    ) as "new", 
    
    -- Increase in spending from users who spent more this day compared to last day.
    sum(
      case 
        when tm.dt != tm.first_dt and tm.user_id is not null and lm.user_id is not null and tm.inc_amt > lm.inc_amt and lm.inc_amt > 0 then tm.inc_amt - lm.inc_amt 
        else 0 
      end
    ) as expansion, 
    
    -- Revenue from users who were not active last day but came back and made a purchase this day.
    sum(
      case 
        when tm.user_id is not null and (lm.user_id is null or lm.inc_amt = 0) and tm.inc_amt > 0 and tm.first_dt != tm.dt then tm.inc_amt 
        else 0 
      end
    ) as resurrected, 
    
    -- Decrease in spending from users who spent less this day compared to last day.
    -1 * sum(
      case 
        when tm.dt != tm.first_dt and tm.user_id is not null and lm.user_id is not null and tm.inc_amt < lm.inc_amt and tm.inc_amt > 0 then lm.inc_amt - tm.inc_amt 
        else 0 
      end
    ) as contraction, 
    
    -- Revenue lost from users who were active last day but did not make a purchase this day.
    -1 * sum(
      case 
        when lm.inc_amt > 0 and (tm.user_id is null or tm.inc_amt = 0) then lm.inc_amt 
        else 0 
      end
    ) as churned 
    from
      dayly_rev tm 
      full outer join dayly_rev lm on tm.user_id = lm.user_id
      and tm.dt = lm.dt + interval '1 day'

  group by 1

)

select 
  base.*,
  -- preceding_rev is used to calculate day-over-day retention rate.
  lag(rev) over (order by day) as preceding_rev

from base

where day <= date_trunc('day', current_date at time zone 'America/Sao_Paulo')