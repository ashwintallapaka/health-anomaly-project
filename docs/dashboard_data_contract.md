# Dashboard Data Contract

## Connection

MongoDB Atlas, database name: `health_anomaly`

Connection string (yours, generated separately — see teammate access setup):
mongodb+srv://YOUR_USER:YOUR_PASSWORD@healthdata.1bxocja.mongodb.net/?appName=HealthData

Load via `python-dotenv` — don't hardcode it. `.env` file, key `MONGO_URI`.

## Collections

### `latest_vitals`
One document per user, continuously overwritten (upsert) on every new reading.
Update frequency: roughly every 2 seconds per active user.

Fields:
- `user_id` (string)
- `timestamp` (string, ISO 8601)
- `heart_rate` (int)
- `body_temp_c` (float)
- `spo2` (int)
- `is_injected_anomaly` (bool) — ground truth, for validation only, not for display logic
- `detected_anomaly` (bool) — what the system actually flagged

Sample document:
--- latest_vitals sample ---
{'_id': ObjectId('6a9f25fc41cfc9081753bee6'), 'user_id': 'P002', 'body_temp_c': 38.400001525878906, 'detected_anomaly': True, 'heart_rate': 135, 'is_injected_anomaly': True, 'spo2': 89, 'timestamp': '2026-09-07T21:16:56'}
--- alerts sample ---
{'_id': 'P001|2026-09-07T21:00:44', 'user_id': 'P001', 'timestamp': '2026-09-07T21:00:44', 'heart_rate': 135, 'body_temp_c': 38.400001525878906, 'spo2': 89, 'rule': 'threshold', 'is_injected_anomaly': True}
collections: ['alerts', 'latest_vitals']
alert count: 110
latest_vitals count: 3
### `alerts`
One document per detected anomaly. Grows over time, never overwritten
(idempotent — `_id` is `user_id|timestamp`, so reprocessing doesn't duplicate).

Fields:
- `_id` (string, `user_id|timestamp`)
- `user_id` (string)
- `timestamp` (string, ISO 8601)
- `heart_rate` (int)
- `body_temp_c` (float)
- `spo2` (int)
- `rule` (string) — currently always `"threshold"`; will add `"zscore"` once baselines are live
- `is_injected_anomaly` (bool) — ground truth

Sample document:
[PASTE REAL OUTPUT FROM STEP 1 HERE]

## Coming later (not yet in the data — don't build against these yet)
- `baselines` collection (per-user mean/std) — batch layer not built yet
- `z_score` and `severity` fields on alerts — depends on baselines existing
- I'll update this doc the moment those land

## Suggested starter queries

Get all users' current vitals:
python
docs = list(db.latest_vitals.find())


Get the 20 most recent alerts:
python
docs = list(db.alerts.find().sort("timestamp", -1).limit(20))


Count alerts per user (for a summary widget):
python
pipeline = [{"$group": {"_id": "$user_id", "count": {"$sum": 1}}}]
counts = list(db.alerts.aggregate(pipeline))


## Refresh strategy
Data updates every ~2 seconds. A dashboard polling Mongo every 2-3 seconds
is sufficient — no need for anything fancier at this scale. If you want
push-based updates instead of polling, MongoDB change streams are the
proper mechanism, but polling is simpler and fine for a demo.
