create or replace view weekly_quick_ratio_view as
select 
    week,
    case 
        when churned != 0 then 
            round((-1.0 * (new + resurrected)::numeric / churned::numeric)::numeric, 2)
        else 0 
    end as quick_ratio,
    session_id
from wau_view
where week <= date_trunc('week', current_date at time zone 'America/Sao_Paulo')
order by week; 