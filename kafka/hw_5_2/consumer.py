import json
import os
import logging

from datetime import datetime
from typing import List, Tuple, Optional
from confluent_kafka import Consumer, KafkaException
from clickhouse_driver import Client


TOPIC = os.getenv("KAFKA_TOPIC", "user_logins")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ch_sink_group")

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", "9008"))
CH_DATABASE = os.getenv("CH_DATABASE", "analytics")

CH_USER = os.getenv("CH_USER", "app")
CH_PASSWORD = os.getenv("CH_PASSWORD", "app")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))


DDL = """
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
"""


def _parse_event_time(value: str) -> datetime:
    # Поддержка ISO 8601 с "Z" на конце: 2026-01-11T10:12:13Z
    value = value.replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("consumer")

    log.info("Starting consumer...")
    log.info(
        "Config: topic=%s group_id=%s kafka=%s clickhouse=%s:%s db=%s batch_size=%s",
        TOPIC, KAFKA_GROUP_ID, KAFKA_BOOTSTRAP, CH_HOST, CH_PORT, CH_DATABASE, BATCH_SIZE
    )

    # ClickHouse
    ch = Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
    )

    # Инициализация схемы (для самодостаточности consumer)
    for stmt in DDL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            ch.execute(stmt)
    log.info("ClickHouse schema ensured.")

    # Kafka consumer
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })

    c.subscribe([TOPIC])
    log.info("Subscribed to topic=%s", TOPIC)

    buffer: List[Tuple[int, int, str, datetime, str]] = []
    last_msg: Optional[object] = None

    def flush() -> None:
        nonlocal buffer, last_msg
        if not buffer:
            return

        ch.execute(
            "INSERT INTO analytics.user_logins (event_id, user_id, event_type, event_time, payload) VALUES",
            buffer
        )

        if last_msg is not None:
            # Коммит оффсета только после успешной вставки
            c.commit(message=last_msg, asynchronous=False)

        log.info("Inserted %d rows into ClickHouse and committed offset.", len(buffer))
        buffer.clear()

    try:
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            data = json.loads(msg.value().decode("utf-8"))

            event_id = int(data["event_id"])
            user_id = int(data["user_id"])
            event_type = str(data["event_type"])
            event_time = _parse_event_time(str(data["event_time"]))
            payload = json.dumps(data.get("payload", {}), ensure_ascii=False)

            buffer.append((event_id, user_id, event_type, event_time, payload))
            last_msg = msg

            if len(buffer) >= BATCH_SIZE:
                flush()

    except KeyboardInterrupt:
        log.info("Shutdown requested (KeyboardInterrupt). Flushing remaining messages...")
    except Exception:
        log.exception("Fatal error in consumer loop.")
        raise
    finally:
        try:
            flush()
        except Exception:
            log.exception("Failed to flush remaining messages during shutdown.")
        finally:
            c.close()
            log.info("Consumer closed.")


if __name__ == "__main__":
    main()
