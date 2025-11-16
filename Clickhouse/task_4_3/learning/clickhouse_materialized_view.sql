--создаем таблицу events
CREATE TABLE events (
    user_id UInt32
    , event_type String
    , event_time DateTime
) ENGINE = MergeTree()
ORDER BY
event_time;

--создаем таблицу event_counts
CREATE TABLE event_counts (
    event_type String
    , count UInt32
) ENGINE = SummingMergeTree()
ORDER BY
event_type;

-- создаем materialized view с названием mv_event_counts.
-- Извлекаем данные поля event_type и счиатем их количество count() с группировкой по полю event_type
-- Записываем эти данные в таблицу event_counts
CREATE MATERIALIZED VIEW mv_event_counts
TO event_counts AS
SELECT
    event_type
    , count() AS count
FROM
    events
GROUP BY
    event_type;

-- Вставляем данные в таблицу events
INSERT
    INTO 
    events
VALUES (1, 'click', now())
       , (2, 'view', now())
       , (3, 'click', now());

-- смотрим данные в таблице events
SELECT
    *
FROM
    events;
-- смотрим данные в таблице event_counts
SELECT
    *
FROM
    event_counts;


