create or replace view weekly_revenue_quick_ratio_view as
select 
    week,
    case 
        when (churned + contraction) != 0 then 
            round((-1.0 * (new + resurrected + expansion)::numeric / (churned + contraction)::numeric)::numeric, 2)
        else 0 
    end as quick_ratio,
    session_id
from wrr_view
where week <= date_trunc('week', current_date at time zone 'America/Sao_Paulo')
order by week; 