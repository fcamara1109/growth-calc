create or replace view weekly_retention_view as
with wau_data as (
    select
        week,
        retained,
        preceding_wau,
        session_id
    from wau_view
)
select 
    week,
    retained,
    preceding_wau,
    case 
        when preceding_wau > 0 then 
            round((retained::numeric / preceding_wau::numeric) * 100, 2)
        else 0 
    end as retention_rate,
    session_id
from wau_data
where week <= date_trunc('week', current_date at time zone 'America/Sao_Paulo')
order by week; 