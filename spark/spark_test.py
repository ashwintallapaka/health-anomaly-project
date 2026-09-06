from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HealthAnomalyProject").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"Spark version: {spark.version}")

data = [
    ("P001", 75, 98),
    ("P002", 135, 89),
]
df = spark.createDataFrame(data, ["patient_id", "heart_rate", "spo2"])
df.show()

spark.stop()
