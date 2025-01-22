-- Upload metrics
create table metrics_uploads (
    id uuid default uuid_generate_v4() primary key,
    timestamp timestamptz not null,
    session_id uuid not null,
    file_size_bytes bigint not null,
    processing_time_ms float not null,
    success boolean not null,
    error text
);

-- Create indexes
create index idx_uploads_timestamp on metrics_uploads(timestamp);
