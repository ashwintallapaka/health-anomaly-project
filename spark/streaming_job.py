from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, BooleanType
import sys
sys.path.append("spark")
from detection_rules import is_anomalous

spark = SparkSession.builder.appName("HealthStreamDetection").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("user_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("heart_rate", IntegerType()) \
    .add("body_temp_c", FloatType()) \
    .add("spo2", IntegerType()) \
    .add("is_injected_anomaly", BooleanType())

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "health-data") \
    .option("startingOffsets", "latest") \
    .load()

parsed = raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

anomaly_udf = udf(is_anomalous, BooleanType())
scored = parsed.withColumn(
    "detected_anomaly",
    anomaly_udf(col("spo2"), col("body_temp_c"), col("heart_rate"))
)

query = scored.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
