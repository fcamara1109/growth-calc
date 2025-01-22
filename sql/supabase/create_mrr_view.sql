create or replace view mrr_view as
select
    coalesce(tm.month, lm.month + interval '1 month') as month,
    sum(tm.inc_amt) as rev,
    sum(
        case 
            when tm.user_id is not null and lm.user_id is not null and tm.inc_amt >= lm.inc_amt then lm.inc_amt 
            when tm.user_id is not null and lm.user_id is not null and tm.inc_amt < lm.inc_amt then tm.inc_amt 
            else 0 
        end
    ) as retained,
    sum(
        case 
            when tm.first_month = tm.month then tm.inc_amt 
            else 0 
        end
    ) as "new",
    sum(
        case 
            when tm.month != tm.first_month and tm.user_id is not null and lm.user_id is not null 
            and tm.inc_amt > lm.inc_amt and lm.inc_amt > 0 then tm.inc_amt - lm.inc_amt 
            else 0 
        end
    ) as expansion,
    sum(
        case 
            when tm.user_id is not null and (lm.user_id is null or lm.inc_amt = 0) 
            and tm.inc_amt > 0 and tm.first_month != tm.month then tm.inc_amt 
            else 0 
        end
    ) as resurrected,
    -1 * sum(
        case 
            when tm.month != tm.first_month and tm.user_id is not null and lm.user_id is not null 
            and tm.inc_amt < lm.inc_amt and tm.inc_amt > 0 then lm.inc_amt - tm.inc_amt 
            else 0 
        end
    ) as contraction,
    -1 * sum(
        case 
            when lm.inc_amt > 0 and (tm.user_id is null or tm.inc_amt = 0) then lm.inc_amt 
            else 0 
        end
    ) as churned,
    lag(sum(tm.inc_amt)) over (partition by coalesce(tm.session_id, lm.session_id) order by coalesce(tm.month, lm.month + interval '1 month')) as preceding_rev,
    coalesce(tm.session_id, lm.session_id) session_id
from daily_rev_view tm 
full outer join daily_rev_view lm on tm.user_id = lm.user_id
    and tm.month = lm.month + interval '1 month'
    and tm.session_id = lm.session_id
where coalesce(tm.month, lm.month + interval '1 month') <= date_trunc('month', current_date at time zone 'America/Sao_Paulo')
group by 1, coalesce(tm.session_id, lm.session_id)
order by 1;