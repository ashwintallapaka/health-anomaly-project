import os, csv, sys, subprocess
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

BUCKET = "gs://health-anomaly-data"
FIELDS = ["user_id", "timestamp", "heart_rate", "body_temp_c", "spo2"]

# default: yesterday (UTC); override with a date arg
day = sys.argv[1] if len(sys.argv) > 1 else \
      (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
next_day = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

db = MongoClient(os.environ["MONGO_URI"])["health_anomaly"]
docs = list(db.readings_history.find(
    {"timestamp": {"$gte": f"{day}T00:00:00", "$lt": f"{next_day}T00:00:00"}},
    {"_id": 0, "ingested_at": 0},
))

if not docs:
    print(f"No readings found for {day} — nothing to export")
    sys.exit(0)

local = f"/tmp/readings_{day}.csv"
with open(local, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
    w.writeheader()
    w.writerows(docs)

dest = f"{BUCKET}/historical/date={day}/readings.csv"
subprocess.run(["gcloud", "storage", "cp", local, dest], check=True)
print(f"Exported {len(docs)} readings for {day} -> {dest}")
