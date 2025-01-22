create or replace view monthly_revenue_retention_view as
with mrr_data as (
    select
        month,
        retained,
        preceding_rev,
        session_id
    from mrr_view
)
select 
    month,
    retained,
    preceding_rev,
    case 
        when preceding_rev > 0 then 
            round((retained::numeric / preceding_rev::numeric) * 100, 2)
        else 0 
    end as retention_rate,
    session_id
from mrr_data
where month <= date_trunc('month', current_date at time zone 'America/Sao_Paulo')
order by month; 