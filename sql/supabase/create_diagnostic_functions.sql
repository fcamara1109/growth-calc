create or replace function diagnose_daily_cohorts()
returns table (
    step text,
    duration interval,
    row_count bigint
) 
security definer
set search_path = public
language plpgsql
as $$
declare
    start_time timestamp;
    step_duration interval;
    step_count bigint;
begin
    -- Step 1: Check cohorts CTE
    start_time := clock_timestamp();
    create temp table temp_cohorts as
    select
        first_dt,
        dt as active_day,
        days_since_first,
        count(distinct user_id) as users,
        sum(inc_amt) as inc_amt,
        session_id
    from daily_rev_view
    group by 1,2,3,6;
    
    step_duration := clock_timestamp() - start_time;
    select count(*) into step_count from temp_cohorts;
    return query select 'Step 1: Cohorts'::text, step_duration, step_count;
    
    -- Step 2: Check cohort_sizes CTE
    start_time := clock_timestamp();
    create temp table temp_cohort_sizes as
    select * from temp_cohorts where days_since_first = 0;
    
    step_duration := clock_timestamp() - start_time;
    select count(*) into step_count from temp_cohort_sizes;
    return query select 'Step 2: Cohort Sizes'::text, step_duration, step_count;
    
    -- Step 3: Check all_days CTE
    start_time := clock_timestamp();
    create temp table temp_all_days as
    select
        cs.first_dt,
        (cs.first_dt + (day_number * interval '1 day'))::date as active_day,
        cs.session_id
    from temp_cohort_sizes cs,
        generate_series(0, 90) as day_number;
    
    step_duration := clock_timestamp() - start_time;
    select count(*) into step_count from temp_all_days;
    return query select 'Step 3: All Days'::text, step_duration, step_count;
    
    -- Step 4: Check all_cohorts CTE
    start_time := clock_timestamp();
    create temp table temp_all_cohorts as
    select
        am.*,
        am.active_day - am.first_dt as days_since_first,
        c.users,
        c.inc_amt
    from temp_all_days am
    left join temp_cohorts c 
        on am.first_dt = c.first_dt
        and am.active_day = c.active_day
        and am.session_id = c.session_id;
    
    step_duration := clock_timestamp() - start_time;
    select count(*) into step_count from temp_all_cohorts;
    return query select 'Step 4: All Cohorts'::text, step_duration, step_count;
    
    -- Step 5: Check cumulative CTE
    start_time := clock_timestamp();
    create temp table temp_cumulative as
    select
        c1.first_dt,
        c1.active_day,
        c1.days_since_first,
        c1.users,
        c1.inc_amt,
        cs.users as cohort_num_users,
        sum(c2.inc_amt) as cum_amt,
        c1.session_id
    from temp_all_cohorts c1
    join temp_all_cohorts c2 
        on c1.first_dt = c2.first_dt
        and c2.days_since_first <= c1.days_since_first
        and c1.session_id = c2.session_id
    join temp_cohort_sizes cs 
        on cs.first_dt = c1.first_dt
        and cs.session_id = c1.session_id
    group by 1,2,3,4,5,6,8;
    
    step_duration := clock_timestamp() - start_time;
    select count(*) into step_count from temp_cumulative;
    return query select 'Step 5: Cumulative'::text, step_duration, step_count;
    
    -- Cleanup
    drop table temp_cohorts;
    drop table temp_cohort_sizes;
    drop table temp_all_days;
    drop table temp_all_cohorts;
    drop table temp_cumulative;
end;
$$;

-- Grant execute permission
grant execute on function diagnose_daily_cohorts() to postgres, authenticated, anon; 