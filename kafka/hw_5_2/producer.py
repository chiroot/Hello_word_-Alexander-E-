import json
import os
import time
import logging
import psycopg2

from typing import List, Dict, Any, Optional
from psycopg2.extras import RealDictCursor
from confluent_kafka import Producer


TOPIC = os.getenv("KAFKA_TOPIC", "user_logins")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "1.0"))

PG_DSN = os.getenv(
    "PG_DSN",
    "host=localhost port=5437 dbname=app user=app password=app"
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")


def fetch_unsent_events(cur) -> List[Dict[str, Any]]:
    cur.execute(
        """
        SELECT id, user_id, event_type, event_time, payload
        FROM user_logins
        WHERE sent_to_kafka = FALSE
        ORDER BY id
        FOR UPDATE SKIP LOCKED
        LIMIT %s
        """,
        (BATCH_SIZE,)
    )
    return cur.fetchall()


def mark_sent(cur, ids: List[int]) -> None:
    cur.execute(
        "UPDATE user_logins SET sent_to_kafka = TRUE WHERE id = ANY(%s)",
        (ids,)
    )


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("producer")

    log.info("Starting producer...")
    log.info(
        "Config: topic=%s kafka=%s batch_size=%s poll_interval=%ss pg_dsn=%s",
        TOPIC, KAFKA_BOOTSTRAP, BATCH_SIZE, POLL_INTERVAL_SECONDS, PG_DSN
    )

    delivery_errors: List[str] = []
    delivered_count: int = 0
    last_delivery_error: Optional[str] = None

    def delivery_report(err, msg) -> None:
        nonlocal delivered_count, last_delivery_error
        if err is not None:
            last_delivery_error = str(err)
            delivery_errors.append(last_delivery_error)
        else:
            delivered_count += 1

    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "retries": 10,
        "retry.backoff.ms": 300,
        "delivery.timeout.ms": 120000,
    }
    p = Producer(producer_conf)

    # PostgreSQL
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False
    log.info("Connected to Postgres.")

    # Быстрый health-check Kafka: metadata запрос (не гарантирует существование топика, но проверяет доступность брокера)
    try:
        md = p.list_topics(timeout=5.0)
        brokers = list(md.brokers.keys()) if md is not None else []
        log.info("Connected to Kafka. Brokers=%s", brokers if brokers else "unknown")
    except Exception:
        log.exception("Kafka metadata request failed (bootstrap issue likely).")
        raise

    total_sent_rows: int = 0
    total_cycles: int = 0

    try:
        while True:
            total_cycles += 1
            cycle_start_ts = time.time()

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                try:
                    delivery_errors.clear()
                    delivered_count = 0
                    last_delivery_error = None

                    events = fetch_unsent_events(cur)
                    if not events:
                        conn.commit()
                        log.debug("No unsent events. Sleeping %ss", POLL_INTERVAL_SECONDS)
                        time.sleep(POLL_INTERVAL_SECONDS)
                        continue

                    ids: List[int] = []
                    for row in events:
                        ids.append(int(row["id"]))
                        message = {
                            "event_id": int(row["id"]),
                            "user_id": int(row["user_id"]),
                            "event_type": str(row["event_type"]),
                            "event_time": row["event_time"].isoformat(),
                            "payload": row["payload"],
                        }

                        p.produce(
                            topic=TOPIC,
                            key=str(row["id"]),
                            value=json.dumps(message, ensure_ascii=False),
                            on_delivery=delivery_report,
                        )

                        # даём librdkafka обработать delivery callbacks
                        p.poll(0)

                    remaining = p.flush(30)
                    if remaining != 0:
                        raise RuntimeError(
                            f"Kafka flush timeout: {remaining} message(s) undelivered"
                        )

                    if delivery_errors:
                        raise RuntimeError(
                            f"Kafka delivery errors (first 3): {delivery_errors[:3]}"
                        )

                    mark_sent(cur, ids)
                    conn.commit()

                    total_sent_rows += len(ids)
                    elapsed = time.time() - cycle_start_ts

                    # Главный “сигнал”, что всё работает: отправили -> доставили -> отметили в Postgres
                    log.info(
                        "Cycle OK: fetched=%d produced=%d delivered=%d marked_sent=%d elapsed=%.3fs total_sent=%d",
                        len(events),
                        len(ids),
                        delivered_count,
                        len(ids),
                        elapsed,
                        total_sent_rows,
                    )

                except Exception:
                    conn.rollback()
                    log.exception("Cycle FAILED. Transaction rolled back.")
                    raise

    except KeyboardInterrupt:
        log.info("Shutdown requested (KeyboardInterrupt).")
    finally:
        try:
            # финальный flush на всякий случай (обычно уже пусто, но безопасно)
            p.flush(5)
        except Exception:
            log.exception("Final Kafka flush failed during shutdown.")
        finally:
            conn.close()
            log.info("Postgres connection closed. Producer stopped.")


if __name__ == "__main__":
    main()
