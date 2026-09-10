from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys


spark = SparkSession.builder \
    .appName("HealthAnomalyBatchProcessing") \
    .getOrCreate()


# Use command-line paths on Dataproc.
# Use local paths when running locally.
if len(sys.argv) >= 3:
    input_path = sys.argv[1]
    output_path = sys.argv[2]
else:
    input_path = "data/historical_vitals.csv"
    output_path = "batch_output"


print("Reading historical data from:", input_path)


# Read historical CSV
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)


print("Input historical data:")
df.show(truncate=False)


# ---------------------------------------------------------
# 1. Fixed threshold anomaly detection
# ---------------------------------------------------------
df = df.withColumn(
    "threshold_anomaly",
    (F.col("spo2") < 92) |
    (F.col("body_temp_c") > 38.0) |
    (F.col("heart_rate") < 50) |
    (F.col("heart_rate") > 120)
)


# ---------------------------------------------------------
# 2. Compute personal baseline statistics
# ---------------------------------------------------------
baselines = df.groupBy("user_id").agg(
    F.round(F.avg("heart_rate"), 2).alias("mean_heart_rate"),
    F.round(F.stddev("heart_rate"), 2).alias("stddev_heart_rate"),

    F.round(F.avg("body_temp_c"), 2).alias("mean_body_temp_c"),
    F.round(F.stddev("body_temp_c"), 2).alias("stddev_body_temp_c"),

    F.round(F.avg("spo2"), 2).alias("mean_spo2"),
    F.round(F.stddev("spo2"), 2).alias("stddev_spo2")
)


print("Personal Baselines:")
baselines.show(truncate=False)


# ---------------------------------------------------------
# 3. Join baselines back to individual readings
# ---------------------------------------------------------
df_with_baselines = df.join(
    baselines,
    on="user_id",
    how="left"
)


# ---------------------------------------------------------
# 4. Compute z-scores
# ---------------------------------------------------------
df_with_zscores = df_with_baselines \
    .withColumn(
        "heart_rate_z",
        F.when(
            F.col("stddev_heart_rate") != 0,
            (F.col("heart_rate") - F.col("mean_heart_rate")) /
            F.col("stddev_heart_rate")
        ).otherwise(F.lit(0.0))
    ) \
    .withColumn(
        "body_temp_z",
        F.when(
            F.col("stddev_body_temp_c") != 0,
            (F.col("body_temp_c") - F.col("mean_body_temp_c")) /
            F.col("stddev_body_temp_c")
        ).otherwise(F.lit(0.0))
    ) \
    .withColumn(
        "spo2_z",
        F.when(
            F.col("stddev_spo2") != 0,
            (F.col("spo2") - F.col("mean_spo2")) /
            F.col("stddev_spo2")
        ).otherwise(F.lit(0.0))
    )


# ---------------------------------------------------------
# 5. Personal baseline anomaly detection
# ---------------------------------------------------------
df_with_zscores = df_with_zscores.withColumn(
    "baseline_anomaly",
    (F.abs(F.col("heart_rate_z")) > 3) |
    (F.abs(F.col("body_temp_z")) > 3) |
    (F.abs(F.col("spo2_z")) > 3)
)


# ---------------------------------------------------------
# 6. Combined anomaly
# ---------------------------------------------------------
df_with_zscores = df_with_zscores.withColumn(
    "is_anomaly",
    F.col("threshold_anomaly") | F.col("baseline_anomaly")
)


print("Z-Score Detection Results:")
df_with_zscores.select(
    "user_id",
    "timestamp",
    "heart_rate",
    F.round("heart_rate_z", 2).alias("heart_rate_z"),
    "body_temp_c",
    F.round("body_temp_z", 2).alias("body_temp_z"),
    "spo2",
    F.round("spo2_z", 2).alias("spo2_z"),
    "threshold_anomaly",
    "baseline_anomaly",
    "is_anomaly"
).show(100, truncate=False)


# ---------------------------------------------------------
# 7. Final per-user batch summary
# ---------------------------------------------------------
summary = df_with_zscores.groupBy("user_id").agg(
    F.count("*").alias("total_readings"),

    F.round(F.avg("heart_rate"), 2).alias("avg_heart_rate"),
    F.round(F.stddev("heart_rate"), 2).alias("stddev_heart_rate"),
    F.min("heart_rate").alias("min_heart_rate"),
    F.max("heart_rate").alias("max_heart_rate"),

    F.round(F.avg("body_temp_c"), 2).alias("avg_body_temp_c"),
    F.round(F.stddev("body_temp_c"), 2).alias("stddev_body_temp_c"),
    F.max("body_temp_c").alias("max_body_temp_c"),

    F.round(F.avg("spo2"), 2).alias("avg_spo2"),
    F.round(F.stddev("spo2"), 2).alias("stddev_spo2"),
    F.min("spo2").alias("min_spo2"),

    F.sum(
        F.when(F.col("threshold_anomaly"), 1).otherwise(0)
    ).alias("threshold_anomaly_count"),

    F.sum(
        F.when(F.col("baseline_anomaly"), 1).otherwise(0)
    ).alias("baseline_anomaly_count"),

    F.sum(
        F.when(F.col("is_anomaly"), 1).otherwise(0)
    ).alias("anomaly_count")
)


print("Batch Processing Summary:")
summary.show(truncate=False)


# ---------------------------------------------------------
# 8. Save results
# ---------------------------------------------------------
summary.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)


print("Batch processing completed successfully.")
print("Output saved to:", output_path)


spark.stop()
