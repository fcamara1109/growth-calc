create or replace view daily_revenue_retention_view as
select 
    day,
    retained,
    preceding_rev,
    case 
        when preceding_rev > 0 then 
            round((retained::numeric / preceding_rev::numeric) * 100, 2)
        else 0 
    end as retention_rate,
    session_id
from drr_view
where day <= date_trunc('day', current_date at time zone 'America/Sao_Paulo')
order by day; 