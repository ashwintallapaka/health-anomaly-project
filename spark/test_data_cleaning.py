from pyspark.sql import SparkSession
from data_cleaning import clean_readings, is_plausible_reading

spark = SparkSession.builder.appName("DataCleaningTest").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Deliberately dirty sample data: one normal reading, one exact duplicate,
# one missing a value, one with an impossible heart rate, one with an
# impossible SpO2, and one that's a real (plausible) anomaly that must
# survive cleaning untouched.
data = [
    ("P001", "2026-09-01T10:00:00", 74,  36.7, 98),   # normal
    ("P001", "2026-09-01T10:00:00", 74,  36.7, 98),   # exact duplicate -> dropped
    ("P002", "2026-09-01T10:05:00", None, 36.8, 97),  # missing heart_rate -> dropped
    ("P002", "2026-09-01T10:10:00", 400, 36.8, 97),   # impossible heart rate -> dropped
    ("P003", "2026-09-01T10:15:00", 80,  36.9, 150),  # impossible SpO2 -> dropped
    ("P003", "2026-09-01T10:20:00", 135, 38.4, 89),   # real anomaly, plausible -> kept
]

df = spark.createDataFrame(data, ["user_id", "timestamp", "heart_rate", "body_temp_c", "spo2"])

print("Before cleaning:")
df.show(truncate=False)

clean_df, report = clean_readings(df)

print("After cleaning:")
clean_df.orderBy("user_id", "timestamp").show(truncate=False)

assert report["input_rows"] == 6
assert report["dropped_missing_or_unparseable"] == 1
assert report["dropped_duplicates"] == 1
assert report["dropped_implausible"] == 2
assert report["output_rows"] == 2
print("All data_cleaning assertions passed.")

# The pure-Python helper should agree with the Spark-side filtering above.
assert is_plausible_reading(74, 36.7, 98) is True
assert is_plausible_reading(135, 38.4, 89) is True   # anomalous, but plausible
assert is_plausible_reading(400, 36.8, 97) is False  # impossible heart rate
assert is_plausible_reading(80, 36.9, 150) is False  # impossible SpO2
assert is_plausible_reading(None, 36.8, 97) is False # missing value
print("All is_plausible_reading assertions passed.")

spark.stop()
