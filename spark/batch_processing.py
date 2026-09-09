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


# Read historical CSV data
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(input_path)


print("Input historical data:")
df.show(truncate=False)


# Apply the same anomaly thresholds as detection_rules.py:
# SpO2 < 92
# Temperature > 38.0 C
# Heart rate < 50 or > 120
df_with_anomalies = df.withColumn(
    "is_anomaly",
    (F.col("spo2") < 92) |
    (F.col("body_temp_c") > 38.0) |
    (F.col("heart_rate") < 50) |
    (F.col("heart_rate") > 120)
)


print("Historical data with anomaly detection:")
df_with_anomalies.show(truncate=False)


# Generate historical summary for each user
summary = df_with_anomalies.groupBy("user_id").agg(
    F.count("*").alias("total_readings"),

    F.round(F.avg("heart_rate"), 2).alias("avg_heart_rate"),
    F.min("heart_rate").alias("min_heart_rate"),
    F.max("heart_rate").alias("max_heart_rate"),

    F.round(F.avg("body_temp_c"), 2).alias("avg_body_temp_c"),
    F.max("body_temp_c").alias("max_body_temp_c"),

    F.round(F.avg("spo2"), 2).alias("avg_spo2"),
    F.min("spo2").alias("min_spo2"),

    F.sum(
        F.when(F.col("is_anomaly"), 1).otherwise(0)
    ).alias("anomaly_count")
)


print("Batch processing summary:")
summary.show(truncate=False)


# Save batch results
summary.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)


print("Batch processing completed successfully.")
print("Output saved to:", output_path)


spark.stop()
