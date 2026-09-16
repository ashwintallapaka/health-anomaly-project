import os
import json
import time
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.environ["MONGO_URI"]
OUTPUT_PATH = "docs/data.json"
POLL_INTERVAL_SECONDS = 3

client = MongoClient(MONGO_URI)
db = client["health_anomaly"]


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


def export_once():
    docs = list(db.latest_vitals.find({}, {"_id": 0}))
    patients = []
    rule_counts = {"spo2": 0, "temp": 0, "heart_rate": 0}
    flagged = 0
    critical = 0

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

    total = len(patients)
    rate = round((flagged / total) * 100, 1) if total else 0

    payload = {
        "patients": patients,
        "summary": {"total": total, "flagged": flagged, "critical": critical, "rate": rate, "rule_counts": rule_counts},
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"Exported {total} patients, {flagged} flagged, {critical} critical")


if __name__ == "__main__":
    print(f"Polling MongoDB every {POLL_INTERVAL_SECONDS}s, writing to {OUTPUT_PATH}")
    while True:
        try:
            export_once()
        except Exception as e:
            print(f"Export failed: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)
