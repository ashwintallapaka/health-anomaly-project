from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from detection_rules import is_anomalous

spark = SparkSession.builder.appName("DetectionRulesTest").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Sample data: one normal reading, one with low SpO2, one with fever + high HR
data = [
    ("P001", 75, 36.8, 98),   # normal
    ("P002", 135, 37.0, 88),  # high heart rate + low-ish SpO2
    ("P003", 80, 39.2, 96),   # fever
]

df = spark.createDataFrame(data, ["patient_id", "heart_rate", "body_temp_c", "spo2"])

# Register the Python function as a Spark UDF so it runs on the DataFrame
anomaly_udf = udf(is_anomalous, BooleanType())

result = df.withColumn(
    "is_anomaly",
    anomaly_udf(df.spo2, df.body_temp_c, df.heart_rate)
)

result.show()

spark.stop()
