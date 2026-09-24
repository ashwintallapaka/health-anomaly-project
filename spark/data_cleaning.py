"""
Data-cleaning step for the batch pipeline.

Runs on the raw readings before anomaly detection, so that corrupted or
malformed rows don't distort the personal-baseline statistics (mean/stddev)
that batch_processing.py computes, and don't get scored as anomalies by
mistake.

Handles three problems, in order:
  1. Rows missing a required field, or where a value can't be parsed into
     its expected type.
  2. Exact duplicate readings for the same user at the same timestamp
     (e.g. a bridge retry, or export_history_to_gcs.py re-exporting an
     overlapping window).
  3. Physiologically implausible values. This is a different problem from
     a genuine anomaly: an anomaly is a real reading that's dangerous
     (heart_rate=135), while an implausible reading isn't a real vital
     sign at all (heart_rate=400, spo2=150) and would only distort the
     baseline statistics if left in.
"""

from pyspark.sql import functions as F

# Plausible physiological ranges. Anything outside these is treated as bad
# data rather than a genuine health anomaly -- a living person cannot have
# spo2 > 100 or a heart rate of 400 bpm. These are intentionally wider than
# the anomaly thresholds in detection_rules.py: thresholds flag readings
# worth alerting on, this module only throws out readings that could not
# be real in the first place.
VALID_RANGES = {
    "heart_rate":  (20, 250),      # bpm
    "body_temp_c": (30.0, 45.0),   # deg C
    "spo2":        (0, 100),       # percent
}

REQUIRED_COLUMNS = ["user_id", "timestamp", "heart_rate", "body_temp_c", "spo2"]


def clean_readings(df, verbose=True):
    """
    Cleans a raw readings DataFrame -- the schema historical_vitals.csv and
    the nightly GCS export both use -- and returns (clean_df, report),
    where report is a dict of how many rows were removed at each stage,
    for logging and for the report writeup.
    """
    report = {"input_rows": df.count()}

    # 1. Cast to the expected types. A value that can't be cast (e.g. a
    #    truncated line that leaves body_temp_c as an empty string)
    #    becomes null here instead of silently turning into 0 or crashing
    #    a later comparison. Then drop anything missing a required field.
    df = df.withColumn("heart_rate", F.col("heart_rate").cast("int")) \
           .withColumn("body_temp_c", F.col("body_temp_c").cast("double")) \
           .withColumn("spo2", F.col("spo2").cast("int"))

    before = df.count()
    df = df.dropna(subset=REQUIRED_COLUMNS)
    report["dropped_missing_or_unparseable"] = before - df.count()

    # 2. Drop exact duplicate readings for the same user at the same
    #    timestamp. Keep one copy.
    before = df.count()
    df = df.dropDuplicates(["user_id", "timestamp"])
    report["dropped_duplicates"] = before - df.count()

    # 3. Drop physiologically implausible readings -- see VALID_RANGES.
    before = df.count()
    for column, (low, high) in VALID_RANGES.items():
        df = df.filter((F.col(column) >= low) & (F.col(column) <= high))
    report["dropped_implausible"] = before - df.count()

    report["output_rows"] = df.count()

    if verbose:
        print("Data cleaning report:")
        for key, value in report.items():
            print(f"  {key}: {value}")

    return df, report


def is_plausible_reading(heart_rate, body_temp_c, spo2):
    """
    Pure-Python version of the same plausibility check used in
    clean_readings, for use outside Spark -- e.g. as a per-message guard
    in the streaming path (streaming_job.py) before a reading is written
    to MongoDB at all.
    """
    if heart_rate is None or body_temp_c is None or spo2 is None:
        return False

    hr_low, hr_high = VALID_RANGES["heart_rate"]
    temp_low, temp_high = VALID_RANGES["body_temp_c"]
    spo2_low, spo2_high = VALID_RANGES["spo2"]

    return (
        hr_low <= heart_rate <= hr_high and
        temp_low <= body_temp_c <= temp_high and
        spo2_low <= spo2 <= spo2_high
    )
