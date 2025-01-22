with base as (
    select
        coalesce(tm.month, lm.month + interval '1 month') as month,
        count(distinct tm.user_id) as mau,
        count(distinct case when lm.user_id is not null then tm.user_id else null end) as retained,
        count(distinct case when tm.first_month = tm.month then tm.user_id else null end) as "new",
        count(distinct case when tm.first_month != tm.month and lm.user_id is null then tm.user_id else null end) as resurrected,
        -1 * count(distinct case when tm.user_id is null then lm.user_id else null end) as churned
    from
        daily_rev tm full 
        outer join daily_rev lm on tm.user_id = lm.user_id
        and tm.month = lm.month + interval '1 month'
    group by 1
)
select 
    base.*, 
    lag(mau) over (order by month) as preceding_mau
from base
where month <= date_trunc('month', current_date at time zone 'America/Sao_Paulo')
