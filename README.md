# Health Anomaly Detection

A real-time pipeline that simulates wearable vitals, streams them through MQTT and
Kafka, and flags anomalies with a Spark rule engine — built as a course project on
Google Cloud (Dataproc for Spark, Compute Engine for brokers).

```
wearable (simulated) → MQTT → Kafka → Spark (detection_rules.py) → alerts
```

Only the pieces below are wired up so far; see [Status](#status) for what's still
in progress.

## Repo layout

| Path | What it is |
|---|---|
| `mqtt/` | Publisher/subscriber that simulate a wearable sending vitals over MQTT |
| `kafka/` | Producer/consumer that move readings from MQTT into a Kafka topic |
| `spark/` | The anomaly detection rules, a local PySpark smoke test, and a cluster version check |
| `common/schemas.py` | The shared field names, topic names, and message schema every component should agree on |
| `infra/` | Scripts to create/delete the Dataproc cluster used for the Spark side |
| `docs/dashboard.html` | A dashboard visualizing the Spark rule engine against sample vitals (see the `Ashwin-spark-dashboard` branch) |

## Getting started

**Prerequisites:** Python 3.11, a running MQTT broker (e.g. Mosquitto) and Kafka
broker reachable from wherever you run these scripts, and a GCP project with
billing + the Dataproc/Compute/Storage APIs enabled if you're running the Spark
side on Dataproc rather than locally.

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

### Kafka

```bash
export KAFKA_BROKER=localhost:9092   # default
export KAFKA_TOPIC=health-data       # default

python kafka/consumer.py    # run first, in its own terminal
python kafka/producer.py    # sends one sample message and exits
```

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

### Dataproc cluster (for running Spark at scale)

```bash
chmod +x infra/*.sh
./infra/create_cluster.sh    # ~90s to come up; auto-deletes after 2h idle
# ... submit jobs, e.g.:
gcloud dataproc jobs submit pyspark spark/spark_test.py --cluster=health-cluster --region=us-central1
./infra/delete_cluster.sh    # run this when you're done — don't rely on max-idle alone
```

Set a billing budget alert on your GCP project before spinning up clusters —
it's the cheapest insurance against an idle cluster quietly burning credits.

## Status

- ✅ MQTT publisher/subscriber, Kafka producer/consumer, and the Spark rule
  engine all work as standalone pieces.
- ⚠️ **They aren't wired to each other yet** — nothing currently bridges MQTT
  messages into Kafka, or Kafka messages into Spark. That's the next milestone.
- ⚠️ **Field and topic names don't match `common/schemas.py` yet.** For
  example, `mqtt/publisher.py` defaults to topic `wearable/data` and uses
  `user_id`, while `kafka/producer.py` defaults to topic `health-data` and
  uses `patient_id`. `common/schemas.py` is the intended single source of
  truth (topic `wearables/{user_id}/vitals`, field `user_id`, Kafka topics
  `vitals.raw` / `vitals.clean` / `vitals.alerts`) — components need to be
  updated to import from it rather than hardcoding their own names, or the
  Kafka→Spark connector work will hit mismatched fields.
- 🚧 No persistence layer yet (the schema anticipates MongoDB collections and
  HDFS batch paths, but nothing writes to either yet).

## Branches

Team members are working in per-person branches before merging into `main`:
check open branches on GitHub for in-progress work (e.g. Kafka setup, Day 1
notes, the Spark dashboard). Coordinate in your team channel before merging
into `main` to avoid clobbering someone else's in-flight changes.
