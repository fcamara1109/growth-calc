create or replace view monthly_retention_view as
with mau_data as (
    select
        month,
        retained,
        preceding_mau,
        session_id
    from mau_view
)
select 
    month,
    retained,
    preceding_mau,
    case 
        when preceding_mau > 0 then 
            round((retained::numeric / preceding_mau::numeric) * 100, 2)
        else 0 
    end as retention_rate,
    session_id
from mau_data
where month <= date_trunc('month', current_date at time zone 'America/Sao_Paulo')
order by month; 