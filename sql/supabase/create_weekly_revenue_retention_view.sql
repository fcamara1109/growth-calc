create or replace view weekly_revenue_retention_view as
select 
    week,
    retained,
    preceding_rev,
    case 
        when preceding_rev > 0 then 
            round((retained::numeric / preceding_rev::numeric) * 100, 2)
        else 0 
    end as retention_rate,
    session_id
from wrr_view
where week <= date_trunc('week', current_date at time zone 'America/Sao_Paulo')
order by week; 