# Health Anomaly Detection

A real-time pipeline that simulates wearable vitals, streams them through MQTT and
Kafka, flags anomalies with a Spark rule engine, persists everything to MongoDB, and
visualizes it on a live dashboard — built as a course project on Google Cloud
(Dataproc for Spark, Compute Engine for brokers).

```
wearable (simulated) → MQTT → Kafka → Spark (detection_rules.py) → MongoDB → dashboard
```

## Repo layout

| Path | What it is |
|---|---|
| `mqtt/` | Publisher/subscriber that simulates a wearable sending vitals over MQTT |
| `kafka/mqtt_to_kafka_bridge.py` | Bridges the live pipeline: subscribes to the MQTT topic and forwards every message to Kafka unchanged |
| `kafka/producer.py`, `kafka/consumer.py` | Standalone Day-1 demo scripts — **not** part of the live pipeline (see [Status](#status)) |
| `spark/detection_rules.py` | The anomaly detection rules |
| `spark/streaming_job.py` | Spark Structured Streaming job: reads from Kafka, applies the rules, writes to MongoDB |
| `spark/test_detection_rules.py`, `spark/spark_test.py`, `spark/check_version.py` | Local smoke tests / cluster version check |
| `common/schemas.py` | The shared field names, topic names, and message schema every component should agree on |
| `infra/` | Scripts to create/delete the Dataproc cluster used for the Spark side |
| `docs/dashboard_data_contract.md` | The MongoDB schema (`latest_vitals`, `alerts`) that the dashboard reads from |
| `docs/index.html` | The live dashboard, visualizing rule engine output against real MongoDB data — hosted via [GitHub Pages](https://ashwintallapaka.github.io/health-anomaly-project/) |
| `detection_rules.py` (repo root) | Duplicate of `spark/detection_rules.py` — see [Status](#status) |

## Getting started

**Prerequisites:** Python 3.11, a running MQTT broker (e.g., Mosquitto) and Kafka
broker reachable from wherever you run these scripts, a MongoDB Atlas cluster (or
any reachable MongoDB instance) if you want the streaming job to persist data, and
a GCP project with billing + the Dataproc/Compute/Storage APIs enabled if you're
running the Spark side on Dataproc rather than locally.

Each component keeps its own `requirements.txt` — install the ones you need:

```bash
pip install -r mqtt/requirements.txt
pip install -r kafka/requirements.txt
```

### MQTT

```bash
export MQTT_BROKER=localhost      # default
export MQTT_PORT=1883             # default
export MQTT_TOPIC=wearable/data   # default

python mqtt/subscriber.py   # run first, in its own terminal
python mqtt/publisher.py    # sends a simulated reading every 2s, ~20% flagged abnormal
```

### MQTT → Kafka bridge

This is what actually connects the wearable simulator to Kafka — run it alongside
the publisher above:

```bash
python kafka/mqtt_to_kafka_bridge.py
# subscribes to wearable/data on the MQTT broker, forwards every message
# unchanged to the Kafka topic health-data
```

> `kafka/producer.py` and `kafka/consumer.py` are earlier, standalone demo
> scripts and are not part of this flow — see [Status](#status).

### Spark

`spark/detection_rules.py` has no dependencies beyond plain Python — it's the
rule engine itself:

```python
from detection_rules import is_anomalous
is_anomalous(spo2=89, temp=39.2, heart_rate=150)  # -> True
```

| Rule | Threshold |
|---|---|
| SpO₂ floor | `spo2 < 92` |
| Fever | `temp > 38.0`°C |
| Heart rate band | `heart_rate < 50` or `> 120` bpm |

Run it against a small Spark DataFrame locally:

```bash
pip install pyspark==3.5.3
python spark/test_detection_rules.py
```

**Run the live streaming job** (Kafka → Spark → MongoDB): this is what keeps the
dashboard's data current. It reads from the `health-data` Kafka topic, scores every
reading with `is_anomalous`, upserts each user's latest reading into
`health_anomaly.latest_vitals`, and writes an idempotent record to
`health_anomaly.alerts` whenever a rule fires. Field names, collection names, and
sample documents are all documented in `docs/dashboard_data_contract.md`.

```bash
pip install pyspark==3.5.3 pymongo python-dotenv

# create a .env file (not committed) with:
# MONGO_URI=mongodb+srv://<user>:<password>@<cluster-host>/?retryWrites=true&w=majority

python spark/streaming_job.py
```

### Dataproc cluster (for running Spark at scale)

```bash
chmod +x infra/*.sh
./infra/create_cluster.sh    # ~90s to come up; auto-deletes after 2h idle
# ... submit jobs, e.g.:
gcloud dataproc jobs submit pyspark spark/streaming_job.py --cluster=health-cluster --region=us-central1
./infra/delete_cluster.sh    # run this when you're done — don't rely on max-idle alone
```

Set a billing budget alert on your GCP project before spinning up clusters —
it's the cheapest insurance against an idle cluster quietly burning credits.

### Dashboard

`docs/index.html` is a self-contained dashboard that reads MongoDB exports
(`latest_vitals` and `alerts`) per the shape in `docs/dashboard_data_contract.md`,
and shows current patient status, alert history, and the rule engine itself. It's
hosted via GitHub Pages at
**[ashwintallapaka.github.io/health-anomaly-project](https://ashwintallapaka.github.io/health-anomaly-project/)**
(Settings → Pages → deploy from the `main` branch, `/docs` folder).

## Status

- ✅ **The pipeline is wired end-to-end**: `mqtt/publisher.py` → MQTT →
  `kafka/mqtt_to_kafka_bridge.py` → Kafka topic `health-data` →
  `spark/streaming_job.py` → MongoDB (`health_anomaly.latest_vitals`,
  `health_anomaly.alerts`) → `docs/index.html`.
- ⚠️ **Field and topic names still don't match `common/schemas.py`.** The live
  pipeline uses topic `wearable/data` → `health-data` and fields like `user_id`,
  `heart_rate`, `body_temp_c`, `spo2` — that part is consistent across
  `mqtt/publisher.py`, `kafka/mqtt_to_kafka_bridge.py`, and
  `spark/streaming_job.py`. But `common/schemas.py` still describes a different,
  richer intended contract (topic `wearables/{user_id}/vitals`, Kafka topics
  `vitals.raw`/`vitals.clean`/`vitals.alerts`, extra fields like `systolic_bp`,
  `steps`, `lat`/`lon`, and an `ALERT_SCHEMA` with `metric`, `z_score`,
  `severity`, etc.). None of that richer schema is implemented yet — treat
  `common/schemas.py` as the target design, not the current state.
- 🚧 **`kafka/producer.py` and `kafka/consumer.py` are not part of the live
  pipeline.** They're earlier, standalone demo scripts using topic
  `health-data` but fields `patient_id`/`temperature`, which don't match the
  bridge or streaming job. Don't run them expecting them to feed the same
  MongoDB collections the dashboard reads from.
- 🧹 **Cleanup: duplicate `detection_rules.py`.** There's a byte-identical copy
  at the repo root and in `spark/`. Pick one location (probably keep it in
  `spark/`, since that's what `streaming_job.py` imports) and delete the other.
- 🧹 **Alerts are always `rule: "threshold"`.** `ALERT_SCHEMA` anticipates a
  z-score/baseline-based rule too, but `streaming_job.py` only implements the
  fixed threshold checks in `detection_rules.py` — no baseline computation or
  severity scoring yet.
