create or replace view daily_quick_ratio_view as
select 
    day,
    case 
        when churned != 0 then 
            round((-1.0 * (new + resurrected)::numeric / churned::numeric)::numeric, 2)
        else 0 
    end as quick_ratio,
    session_id
from dau_view
where day <= date_trunc('day', current_date at time zone 'America/Sao_Paulo')
order by day; 