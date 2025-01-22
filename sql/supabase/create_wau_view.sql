create or replace view wau_view as
select
    coalesce(tm.week, lm.week + interval '1 week') as week,
    count(distinct tm.user_id) as wau,
    count(distinct case when lm.user_id is not null then tm.user_id else null end) as retained,
    count(distinct case when tm.first_week = tm.week then tm.user_id else null end) as "new",
    count(distinct case when tm.first_week != tm.week and lm.user_id is null then tm.user_id else null end) as resurrected,
    -1 * count(distinct case when tm.user_id is null then lm.user_id else null end) as churned,
    lag(count(distinct tm.user_id)) over (partition by coalesce(tm.session_id, lm.session_id) order by coalesce(tm.week, lm.week + interval '1 week')) as preceding_wau,
    coalesce(tm.session_id, lm.session_id) session_id
from daily_rev_view tm 
full outer join daily_rev_view lm on tm.user_id = lm.user_id
    and tm.week = lm.week + interval '1 week'
    and tm.session_id = lm.session_id
where coalesce(tm.week, lm.week + interval '1 week') <= date_trunc('week', current_date at time zone 'America/Sao_Paulo')
group by 1, coalesce(tm.session_id, lm.session_id)
order by 1; 