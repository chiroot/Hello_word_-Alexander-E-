-- создаем таблицу с событиями пользователей, которые записываются в Clickhouse.
CREATE TABLE user_events (
    user_id UInt32
    , event_type String
    , points_spent UInt32
    , event_time DateTime
) 
ENGINE = MergeTree()
ORDER BY
(
    event_time
    , user_id
)
TTL event_time + INTERVAL 30 DAY DELETE;

-- создаем таблицу для агреггированных данных: 
-- уникальные пользователи (uniq), сумма потраченных баллов (sum), количество действий (count)
CREATE TABLE user_trends (
    event_date Date
    , event_type String
    , uniq_user_state AggregateFunction(uniq, UInt32)
    , total_points_spent_state AggregateFunction(sum, UInt32)
    , total_events_state AggregateFunction(count)
)
ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type) 
TTL event_date + INTERVAL 180 DAY DELETE;

-- создаем материализованное представление, которое при вставке данных в таблицу user_events
-- будет обновлять таблицу с агрегированными данными
CREATE MATERIALIZED VIEW daily_users_mv
TO user_trends
AS
SELECT
    toDate(event_time) AS event_date
    , event_type
    , uniqState(user_id) AS uniq_user_state
    , sumState(points_spent) AS total_points_spent_state
    , countState() AS total_events_state
FROM user_events
GROUP BY event_date
         , event_type;

-- делаем вставку тестовых данных
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY)
, (2, 'signup', 0, now() - INTERVAL 10 DAY)
, (3, 'login', 0, now() - INTERVAL 10 DAY)
, (1, 'login', 0, now() - INTERVAL 7 DAY)
, (2, 'login', 0, now() - INTERVAL 7 DAY)
, (3, 'purchase', 30, now() - INTERVAL 7 DAY)
, (1, 'purchase', 50, now() - INTERVAL 5 DAY)
, (2, 'logout', 0, now() - INTERVAL 5 DAY)
, (4, 'login', 0, now() - INTERVAL 5 DAY)
, (1, 'login', 0, now() - INTERVAL 3 DAY)
, (3, 'purchase', 70, now() - INTERVAL 3 DAY)
, (5, 'signup', 0, now() - INTERVAL 3 DAY)
, (2, 'purchase', 20, now() - INTERVAL 1 DAY)
, (4, 'logout', 0, now() - INTERVAL 1 DAY)
, (5, 'login', 0, now() - INTERVAL 1 DAY)
, (1, 'purchase', 25, now())
, (2, 'login', 0, now())
, (3, 'logout', 0, now())
, (6, 'signup', 0, now())
, (6, 'purchase', 100, now());

-- формируем запрос с группировками по быстрой аналитике по дням
SELECT event_date
       , event_type
       , uniqMerge(uniq_user_state) AS uniq_users
       , sumMerge(total_points_spent_state) AS total_spent
       , countMerge(total_events_state) AS total_actions
FROM user_trends
GROUP BY
    event_date,
    event_type
ORDER BY
    event_date,
    event_type;

-- рассчитываем Retention: сколько пользователей вернулись в течение следующих 7 дней

-- находим день регистрации для каждого клиента (first_event_time) и формируем таблицу с user_id и датой
-- для расчета Retention может быть использована первая дата для каждого user_id, необязательно signup,
-- например, первая дата посещения сайта.
WITH day0 AS (
    SELECT user_id
           , min(event_time) AS first_event_time
    FROM user_events
    WHERE event_type = 'signup'
    GROUP BY user_id
)
-- по данным таблицы day0 считаем сколько пользователей (total_users_day_0) было в конкретный день
, cohorts AS (
    SELECT toDate(first_event_time) AS cohort_day,
           COUNT(*) AS total_users_day_0
    FROM day0
    GROUP BY toDate(first_event_time)
)
-- находим даты, если они есть, в которые пользователь вернулся в течение 7 дней
, returns_day AS (
    SELECT
        d.user_id
        , toDate(d.first_event_time) AS cohort_day
        , toDate(ue.event_time) AS return_day
    FROM
        day0 AS d
    JOIN user_events AS ue ON
        d.user_id = ue.user_id
    WHERE
        ue.event_time > d.first_event_time
        AND ue.event_time <= d.first_event_time + INTERVAL 7 DAY
)
-- считаем количество вернувшихся пользователей в течение 7 дней
, returns_quantity AS (
    SELECT cohort_day
           , uniqExact(user_id) AS returned_in_7_days
    FROM returns_day
    GROUP BY cohort_day
)
-- формируем итоговый запрос с рассчетом Retention
SELECT
    sum(c.total_users_day_0) AS total_users_day_0
    , sum(coalesce(r.returned_in_7_days, 0)) AS returned_in_7_days
    , round(returned_in_7_days * 100.0 / total_users_day_0, 2) AS retention_7d_percent
FROM cohorts AS c
LEFT JOIN returns_quantity AS r ON c.cohort_day = r.cohort_day;

-- второй вариант Retention
-- ....вставить CTE (WITH day0 AS ...) перед выполнением запроса
SELECT c.cohort_day
       , c.total_users_day_0 AS total_users_day_0
       , coalesce(r.returned_in_7_days, 0) AS returned_in_7_days
       , round(returned_in_7_days * 100.0 / total_users_day_0, 2) AS retention_7d_percent
FROM cohorts AS c
LEFT JOIN returns_quantity AS r ON c.cohort_day = r.cohort_day
ORDER BY c.cohort_day;