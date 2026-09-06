"""
Shared data contracts for the Real-Time Health Anomaly Detection pipeline.

This file is the single source of truth for field names, topic names,
and collection names across all four components. If you need to change
something here, message the team first — every component depends on it.
"""

# --- MQTT ---
MQTT_TOPIC = "wearables/{user_id}/vitals"   # e.g. wearables/P001/vitals

# --- Kafka ---
KAFKA_TOPIC_RAW    = "vitals.raw"       # raw readings, straight from MQTT bridge
KAFKA_TOPIC_CLEAN  = "vitals.clean"     # optional: post-validation/cleaning
KAFKA_TOPIC_ALERTS = "vitals.alerts"    # anomalies detected by Spark

# --- MongoDB collections ---
COLL_LATEST_VITALS = "latest_vitals"    # most recent reading per user
COLL_ALERTS        = "alerts"           # anomaly records
COLL_BASELINES     = "baselines"        # per-user mean/std, computed by batch layer

# --- HDFS paths (batch layer) ---
HDFS_RAW_PATH   = "hdfs:///data/raw/vitals"       # partitioned by date
HDFS_BATCH_PATH = "hdfs:///data/batch/fitbit"

# --- The reading message: what every component agrees a "reading" looks like ---
# This is what gets published to MQTT/Kafka and what Spark parses.
READING_SCHEMA = {
    "user_id":             "str",    # e.g. "P001" — consistent across all components
    "timestamp":           "str",    # ISO 8601, e.g. "2026-09-07T14:32:00Z"
    "heart_rate":          "int",
    "body_temp_c":         "float",
    "spo2":                "int",
    "systolic_bp":         "int",
    "diastolic_bp":        "int",
    "steps":               "int",
    "activity_status":     "str",    # e.g. "resting", "walking", "running"
    "lat":                 "float",
    "lon":                 "float",
    "is_injected_anomaly": "bool",   # ground truth flag for validation/precision-recall
}

# --- The alert message: what Spark writes when a rule fires ---
ALERT_SCHEMA = {
    "user_id":             "str",
    "timestamp":           "str",
    "metric":              "str",    # e.g. "spo2", "heart_rate", "body_temp_c"
    "value":               "float",
    "rule":                "str",    # "threshold" or "zscore"
    "baseline_mean":       "float",  # null/None if rule == "threshold"
    "baseline_std":        "float",  # null/None if rule == "threshold"
    "z_score":             "float",  # null/None if rule == "threshold"
    "severity":            "str",    # e.g. "low", "medium", "high"
    "is_injected_anomaly": "bool",
}
