CREATE TABLE IF NOT EXISTS user_logins (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    sent_to_kafka BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_user_logins_unsent
ON user_logins (sent_to_kafka, id)
WHERE sent_to_kafka = FALSE;

-- тестовые данные
INSERT INTO user_logins (user_id, event_type, event_time, payload)
VALUES
(1, 'registration', now() - interval '2 day', '{"source":"web"}'),
(1, 'login',        now() - interval '1 day', '{"ip":"127.0.0.1"}'),
(2, 'login',        now() - interval '3 hour', '{"ip":"10.0.0.2"}');
