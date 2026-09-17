import os
import json
import time
import csv
import io
import subprocess
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.environ["MONGO_URI"]
OUTPUT_PATH = "docs/data.json"
BUCKET = "gs://health-anomaly-data"
POLL_INTERVAL_SECONDS = 3
BATCH_REFRESH_EVERY = 60          # re-read GCS every ~3 minutes, not every poll

client = MongoClient(MONGO_URI)
db = client["health_anomaly"]

_batch_cache = []


def compute_flags_and_severity(hr, temp, spo2):
    flags = []
    if spo2 < 92:
        flags.append("spo2")
    if temp > 38.0:
        flags.append("temp")
    if hr < 50 or hr > 120:
        flags.append("heart_rate")
    severity = "critical" if len(flags) >= 2 else ("warning" if flags else "good")
    return flags, severity


def load_latest_batch():
    """Read the most recent Spark batch output from Cloud Storage."""
    try:
        listing = subprocess.run(
            ["gcloud", "storage", "ls", f"{BUCKET}/batch_output/"],
            capture_output=True, text=True, check=True,
        ).stdout.strip().split("\n")

        dates = sorted(p for p in listing if p.rstrip("/").split("/")[-1][:2] == "20")
        if not dates:
            return []

        raw = subprocess.run(
            ["gcloud", "storage", "cat", f"{dates[-1]}part-*.csv"],
            capture_output=True, text=True, check=True,
        ).stdout

        rows = []
        for r in csv.DictReader(io.StringIO(raw)):
            rows.append({
                "user_id":         r["user_id"],
                "total_readings":  int(r["total_readings"]),
                "avg_heart_rate":  float(r["avg_heart_rate"]),
                "min_heart_rate":  int(float(r["min_heart_rate"])),
                "max_heart_rate":  int(float(r["max_heart_rate"])),
                "avg_body_temp_c": float(r["avg_body_temp_c"]),
                "max_body_temp_c": float(r["max_body_temp_c"]),
                "avg_spo2":        float(r["avg_spo2"]),
                "min_spo2":        int(float(r["min_spo2"])),
                "threshold_count": int(r["threshold_anomaly_count"]),
                "baseline_count":  int(r["baseline_anomaly_count"]),
            })
        rows.sort(key=lambda x: x["user_id"])
        print(f"Loaded batch summary: {len(rows)} patients from {dates[-1]}")
        return rows

    except Exception as e:
        print(f"Could not load batch output: {e}")
        return []


def export_once():
    docs = list(db.latest_vitals.find({}, {"_id": 0}))
    patients = []
    rule_counts = {"spo2": 0, "temp": 0, "heart_rate": 0}
    flagged = critical = 0

    for doc in docs:
        hr, temp, spo2 = doc.get("heart_rate"), doc.get("body_temp_c"), doc.get("spo2")
        flags, severity = compute_flags_and_severity(hr, temp, spo2)
        for f in flags:
            rule_counts[f] += 1
        if flags:
            flagged += 1
        if severity == "critical":
            critical += 1
        patients.append({
            "id": doc.get("user_id"), "hr": hr, "temp": temp, "spo2": spo2,
            "flags": flags, "severity": severity,
        })

    patients.sort(key=lambda p: p["id"] or "")
    total = len(patients)
    rate = round((flagged / total) * 100, 1) if total else 0

    payload = {
        "patients": patients,
        "summary": {"total": total, "flagged": flagged, "critical": critical,
                    "rate": rate, "rule_counts": rule_counts},
        "batch": _batch_cache,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"Exported {total} patients, {flagged} flagged, {critical} critical")


if __name__ == "__main__":
    print(f"Polling MongoDB every {POLL_INTERVAL_SECONDS}s, writing to {OUTPUT_PATH}")
    i = 0
    while True:
        try:
            if i % BATCH_REFRESH_EVERY == 0:
                _batch_cache = load_latest_batch()
            export_once()
        except Exception as e:
            print(f"Export failed: {e}")
        i += 1
        time.sleep(POLL_INTERVAL_SECONDS)
