-- Создаем таблицу users (id, name, email, ROLE, updated_at)
CREATE TABLE users (
    id SERIAL PRIMARY KEY
    , name TEXT
    , email TEXT
    , ROLE TEXT
    , updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/* Создаем таблицу users_audit (id, user_id, сhanged_at, changed_by, field_changed, old_value, new_value), 
в которой будут хранится исторических данные */
CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY
    , user_id INTEGER
    , сhanged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    , changed_by TEXT
    , field_changed TEXT
    , old_value TEXT
    , new_value TEXT
);

/* создаем триггер-функцию audit_user_changes, которая следит за изменением полей в таблице users и
 при наличии изменений сохраняет их в таблицу users_audit */
CREATE OR REPLACE FUNCTION audit_user_changes() RETURNS TRIGGER AS $$
DECLARE
BEGIN
  IF NEW.name IS DISTINCT FROM OLD.name THEN
    INSERT INTO users_audit(user_id, field_changed, old_value, new_value, changed_by)
    VALUES (OLD.id, 'name', OLD.name, NEW.name, current_user);
  END IF;
  
  IF NEW.email IS DISTINCT FROM OLD.email THEN
    INSERT INTO users_audit(user_id, field_changed, old_value, new_value, changed_by)
    VALUES (OLD.id, 'email', OLD.email, NEW.email, current_user);
  END IF;

  IF NEW.role IS DISTINCT FROM OLD.role THEN
    INSERT INTO users_audit(user_id, field_changed, old_value, new_value, changed_by)
    VALUES (OLD.id, 'role', OLD.role, NEW.role, current_user);
  END IF;
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

/* создаем триггер, который вызывает триггер-функцию audit_user_changes 
 перед каждым UPDATE строки в таблице users */
CREATE TRIGGER trigger_audit_user_changes
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION audit_user_changes();

-- включаем расширение pg_cron
CREATE EXTENSION IF NOT EXISTS pg_cron;

/* создаем функцию export_audit_to_csv для экспорта данных из таблицы users_audit в формате .CSV*/
CREATE OR REPLACE FUNCTION export_audit_to_csv() RETURNS void AS $outer$
DECLARE
  path TEXT := '/tmp/users_audit_export_' || to_char(NOW(), 'YYYYMMDD_HH24MI') || '.csv';
BEGIN
  EXECUTE format (
    $inner$
    COPY (
      SELECT user_id, field_changed, old_value, new_value, changed_by, changed_at
      FROM users_audit
      WHERE changed_at >= NOW() - INTERVAL '1 DAY'
      ORDER BY changed_at
    ) TO '%s' WITH CSV HEADER
    $inner$, path
  );
END;
$outer$ LANGUAGE plpgsql;

/* создаем задание daily_audit_export в pg_cron, 
которое каждый день в 03:00 будет запускать функцию export_audit_to_csv() */
SELECT cron.schedule(
  job_name := 'daily_audit_export',
  schedule := '0 3 * * *',
  command := $$SELECT export_audit_to_csv();$$
);

-- смотрим данные в таблице users (таблица пустая)
SELECT * from users;
-- смотрим данные в таблице users_audit (таблица пустая)
SELECT * FROM users_audit;

-- проверяем, что планировщик pg_cron запушен
--SELECT * FROM cron.job;

-- добавляем данные в таблицу users
INSERT INTO users (name, email, role)
VALUES ('Alice', 'alice@example.com', 'user');

-- еще раз смотрим данные в таблице users (появилась новая запись)
SELECT * from users;

-- еще раз смотрим данные в таблице users_audit 
-- таблица пустая, т.к. делали INSERT, а не UPDATE
SELECT * FROM users_audit;

-- обновляем данные в таблице users для пользователя с id = 1
UPDATE users SET name = 'Alice Smith', email = 'alice.smith@example.com' WHERE id = 1;

-- еще раз смотрим данные в таблице users (данные обновились)
SELECT * from users;
-- еще раз смотрим данные в таблице users_audit
-- в таблице отобразились старые и новые данные для пользователя с id = 1
SELECT * FROM users_audit;

-- при необходимости (для проверки) принудительно запускаем экспорт данных из таблицы users_audit в формате csv
--SELECT export_audit_to_csv();

/*просмотр CSV файла в docker:
1. docker exec -it postgres_db bash
2. ls /tmp/users_audit_export_*.csv
3. cat /tmp/users_audit_export_20251105_2141.CSV (выбираем соответствующее название файла)
*/





