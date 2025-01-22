with weekly_rev as (
  select
    user_id,
    week,
    first_week,
    weeks_since_first,
    sum(inc_amt) as inc_amt
  from daily_rev
  group by user_id, week, first_week, weeks_since_first
),

base as (
  select
    coalesce(tm.week, lm.week + interval '1 week') as week,

    -- Total revenue for the week.
    sum(tm.inc_amt) as rev, 

    -- Revenue from users who were active last week and remained active.
    sum(
      case 
        when tm.user_id is not null and lm.user_id is not null and tm.inc_amt >= lm.inc_amt then lm.inc_amt 
        when tm.user_id is not null and lm.user_id is not null and tm.inc_amt < lm.inc_amt then tm.inc_amt 
        else 0 
      end
    ) as retained, 
    
    -- Revenue from new users who were active for the first time this week.
    sum(
      case 
        when tm.first_week = tm.week then tm.inc_amt 
        else 0 
      end
    ) as "new", 
    
    -- Increase in spending from users who spent more this week compared to last week.
    sum(
      case 
        when tm.week != tm.first_week and tm.user_id is not null and lm.user_id is not null and tm.inc_amt > lm.inc_amt and lm.inc_amt > 0 then tm.inc_amt - lm.inc_amt 
        else 0 
      end
    ) as expansion, 
    
    -- Revenue from users who were not active last week but came back and made a purchase this week.
    sum(
      case 
        when tm.user_id is not null and (lm.user_id is null or lm.inc_amt = 0) and tm.inc_amt > 0 and tm.first_week != tm.week then tm.inc_amt 
        else 0 
      end
    ) as resurrected, 
    
    -- Decrease in spending from users who spent less this week compared to last week.
    -1 * sum(
      case 
        when tm.week != tm.first_week and tm.user_id is not null and lm.user_id is not null and tm.inc_amt < lm.inc_amt and tm.inc_amt > 0 then lm.inc_amt - tm.inc_amt 
        else 0 
      end
    ) as contraction, 
    
    -- Revenue lost from users who were active last week but did not make a purchase this week.
    -1 * sum(
      case 
        when lm.inc_amt > 0 and (tm.user_id is null or tm.inc_amt = 0) then lm.inc_amt 
        else 0 
      end
    ) as churned 
    from
      weekly_rev tm 
      full outer join weekly_rev lm on tm.user_id = lm.user_id
      and tm.week = lm.week + interval '1 week'

  group by 1

)

select 
  base.*,
  -- preceding_rev is used to calculate week-over-week retention rate.
  lag(rev) over (order by week) as preceding_rev

from base

where week <= date_trunc('week', current_date at time zone 'America/Sao_Paulo')