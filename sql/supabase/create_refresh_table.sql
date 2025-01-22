-- Create a table to trigger refreshes
create table if not exists refresh_trigger (
    id serial primary key,
    created_at timestamp default now()
);

-- Create function to refresh views with security definer
create or replace function refresh_views()
returns trigger
security definer  -- This makes the function run with owner privileges
set search_path = public  -- Restrict search_path for security
as $$
begin
    refresh materialized view daily_cohorts_view;
    refresh materialized view weekly_cohorts_view;
    refresh materialized view monthly_cohorts_view;
    return new;
end;
$$ language plpgsql;

-- Create trigger
drop trigger if exists refresh_views_trigger on refresh_trigger;
create trigger refresh_views_trigger
after insert on refresh_trigger
for each row
execute function refresh_views();

-- Grant permissions
grant usage on schema public to postgres, authenticated, anon;
grant all on refresh_trigger to postgres, authenticated, anon;
grant execute on function refresh_views() to postgres, authenticated, anon;
grant select on daily_cohorts_view to postgres, authenticated, anon;
grant select on weekly_cohorts_view to postgres, authenticated, anon;
grant select on monthly_cohorts_view to postgres, authenticated, anon;