-- Drop and recreate the trigger table
drop table if exists refresh_trigger cascade;

create table if not exists refresh_trigger (
    id serial primary key,
    created_at timestamp default now(),
    view_name text,
    session_id uuid
);

-- Create function to refresh views with security definer
create or replace function refresh_views()
returns trigger
security definer
set search_path = public
as $$
begin
    -- Only refresh the view specified in the trigger
    case new.view_name
        when 'daily' then
            refresh materialized view concurrently daily_cohorts_view;
        when 'weekly' then
            refresh materialized view concurrently weekly_cohorts_view;
        when 'monthly' then
            refresh materialized view concurrently monthly_cohorts_view;
        else
            raise warning 'Unknown view: %', new.view_name;
    end case;
    
    return new;
exception 
    when others then
        raise warning 'Error refreshing %: %', new.view_name, sqlerrm;
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
