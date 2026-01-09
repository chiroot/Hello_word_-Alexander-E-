from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os


def main():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["my_database"]

    user_events = db["user_events"]
    archived_users = db["archived_users"]

    now = datetime.now()
    reg_cutoff = now - timedelta(days=30)        # зарегистрировались более 30 дней назад
    activity_cutoff = now - timedelta(days=14)   # нет активности последние 14 дней

    # Группируем события по пользователям: берём дату регистрации и дату последнего события
    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "last_event_time": {"$max": "$event_time"},
                "user_info": {"$first": "$user_info"}
            }
        },
        {
            "$match": {
                "user_info.registration_date": {"$lt": reg_cutoff},
                "last_event_time": {"$lt": activity_cutoff}
            }
        }
    ]

    candidates = list(user_events.aggregate(pipeline))
    archived_user_ids = [doc["_id"] for doc in candidates]

    # Готовим документы для архива
    archived_docs = []
    for doc in candidates:
        archived_docs.append({
            "user_id": doc["_id"],
            "user_info": doc.get("user_info", {}),
            "last_event_time": doc.get("last_event_time"),
            "archived_at": now
        })

    # Перемещаем: вставляем в archived_users и удаляем из user_events
    if archived_docs:
        archived_users.insert_many(archived_docs)
        user_events.delete_many({"user_id": {"$in": archived_user_ids}})

    # Отчёт: YYYY-MM-DD.json
    report_date = now.strftime("%Y-%m-%d")
    base_dir = os.path.dirname(os.path.abspath(__file__))
    reports_dir = os.path.join(base_dir, "reports")
    os.makedirs(reports_dir, exist_ok=True)

    report_path = os.path.join(reports_dir, f"{report_date}.json")

    report = {
        "date": report_date,
        "archived_users_count": len(archived_user_ids),
        "archived_user_ids": archived_user_ids
    }

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"Архивировано пользователей: {len(archived_user_ids)}")
    print(f"Отчет сохранен: {report_path}")


if __name__ == "__main__":
    main()