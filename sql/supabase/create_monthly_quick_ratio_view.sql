create or replace view monthly_quick_ratio_view as
select 
    month,
    case 
        when churned != 0 then 
            round((-1.0 * (new + resurrected)::numeric / churned::numeric)::numeric, 2)
        else 0 
    end as quick_ratio,
    session_id
from mau_view
where month <= date_trunc('month', current_date at time zone 'America/Sao_Paulo')
order by month; 