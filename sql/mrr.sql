with monthly_rev as (
  select
    user_id,
    month,
    first_month,
    months_since_first,
    sum(inc_amt) as inc_amt
  from daily_rev
  group by user_id, month, first_month, months_since_first
),

base as (
  select
    coalesce(tm.month, lm.month + interval '1 month') as month,

    -- Total revenue for the month.
    sum(tm.inc_amt) as rev, 

    -- Revenue from users who were active last month and remained active.
    sum(
      case 
        when tm.user_id is not null and lm.user_id is not null and tm.inc_amt >= lm.inc_amt then lm.inc_amt 
        when tm.user_id is not null and lm.user_id is not null and tm.inc_amt < lm.inc_amt then tm.inc_amt 
        else 0 
      end
    ) as retained, 
    
    -- Revenue from new users who were active for the first time this month.
    sum(
      case 
        when tm.first_month = tm.month then tm.inc_amt 
        else 0 
      end
    ) as "new", 
    
    -- Increase in spending from users who spent more this month compared to last month.
    sum(
      case 
        when tm.month != tm.first_month and tm.user_id is not null and lm.user_id is not null and tm.inc_amt > lm.inc_amt and lm.inc_amt > 0 then tm.inc_amt - lm.inc_amt 
        else 0 
      end
    ) as expansion, 
    
    -- Revenue from users who were not active last month but came back and made a purchase this month.
    sum(
      case 
        when tm.user_id is not null and (lm.user_id is null or lm.inc_amt = 0) and tm.inc_amt > 0 and tm.first_month != tm.month then tm.inc_amt 
        else 0 
      end
    ) as resurrected, 
    
    -- Decrease in spending from users who spent less this month compared to last month.
    -1 * sum(
      case 
        when tm.month != tm.first_month and tm.user_id is not null and lm.user_id is not null and tm.inc_amt < lm.inc_amt and tm.inc_amt > 0 then lm.inc_amt - tm.inc_amt 
        else 0 
      end
    ) as contraction, 
    
    -- Revenue lost from users who were active last month but did not make a purchase this month.
    -1 * sum(
      case 
        when lm.inc_amt > 0 and (tm.user_id is null or tm.inc_amt = 0) then lm.inc_amt 
        else 0 
      end
    ) as churned 
    from
      monthly_rev tm 
      full outer join monthly_rev lm on tm.user_id = lm.user_id
      and tm.month = lm.month + interval '1 month'

  group by 1

)

select 
  base.*,
  -- preceding_rev is used to calculate month-over-month retention rate.
  lag(rev) over (order by month) as preceding_rev

from base

where month <= date_trunc('month', current_date at time zone 'America/Sao_Paulo')