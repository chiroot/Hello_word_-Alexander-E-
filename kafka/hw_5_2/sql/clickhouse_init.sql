CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.user_logins
(
    event_id UInt64,
    user_id UInt64,
    event_type String,
    event_time DateTime,
    payload String,
    _ingested_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_ingested_at)
ORDER BY event_id;
