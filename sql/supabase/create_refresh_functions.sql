-- Create function to refresh materialized views
create or replace function refresh_materialized_views()
returns void
language plpgsql
as $$
begin
  refresh materialized view weekly_cohorts_view;
  refresh materialized view daily_cohorts_view;
  -- Add other materialized views here if needed
end;
$$; 