-- Create telemetry_events table to store raw events
CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id       String,
    timestamp      Int64,
    viewer_id      String,
    session_id     String,
    event_type     String,
    video_position Float64,
    bitrate        Int32,
    processed_at   DateTime
) ENGINE = MergeTree()
ORDER BY (viewer_id, session_id, timestamp);

CREATE TABLE IF NOT EXISTS buffer_durations (
    viewer_id        String,
    session_id       String,
    buffer_start_ts  Int64,
    buffer_end_ts    Int64,
    duration_ms      Int64,
    calculated_at    DateTime
) ENGINE = MergeTree()
ORDER BY (viewer_id, session_id, buffer_start_ts);

CREATE TABLE IF NOT EXISTS buffer_timeouts (
    viewer_id       String,
    session_id      String,
    reason          String,   
    event_type      String,
    event_ts        Int64,
    start_ts        Int64,
    elapsed_ms      Int64,
    recorded_at     DateTime
) ENGINE = MergeTree()
ORDER BY (viewer_id, session_id, recorded_at);