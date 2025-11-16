# -*- coding: utf-8 -*-
import psycopg as psycopg2
import clickhouse_connect
from pymongo import MongoClient
from pprint import pprint
from datetime import datetime, timedelta
import json

client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]
archive = db["archived_users"]

now = datetime.now()
fourteen_days_ago = now - timedelta(days=14)
thirty_days_ago = now - timedelta(days=30)

archived_users = list(collection.find(
    {"event_time": {"$lt": fourteen_days_ago},
    "user_info.registration_date":  {"$lt": thirty_days_ago}}
))

for user in archived_users:
    archive.insert_one(user)
    collection.delete_one({"_id": user["_id"]})

# Задать формат отчета
current_date = datetime.now().strftime("%Y-%m-%d")
filename = f"{current_date}.json"
report = {
    "date": current_date,
    "archived_users_count": len(archived_users),
    "archived_user_ids": [user["user_id"] for user in archived_users]
}

# Сохранить в json
with open(filename, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)

print(f"Отчет {filename} создан")