create or replace view daily_retention_view as
with dau_data as (
    select
        day,
        retained,
        preceding_dau,
        session_id
    from dau_view
)
select 
    day,
    retained,
    preceding_dau,
    case 
        when preceding_dau > 0 then 
            round((retained::numeric / preceding_dau::numeric) * 100, 2)
        else 0 
    end as retention_rate,
    session_id
from dau_data
where day <= date_trunc('day', current_date at time zone 'America/Sao_Paulo')
order by day; 