create table error_logs (
    id uuid default uuid_generate_v4() primary key,
    timestamp timestamptz not null,
    session_id uuid not null,
    error_type text not null,
    error_message text not null,
    stack_trace text,
    context jsonb
);

create index idx_errors_timestamp on error_logs(timestamp);