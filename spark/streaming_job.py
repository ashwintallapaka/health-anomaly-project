import os
from dotenv import load_dotenv
load_dotenv()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType, BooleanType
from pymongo import MongoClient
import sys
sys.path.append("spark")
from detection_rules import is_anomalous

MONGO_URI = os.environ["MONGO_URI"]

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


def write_to_mongo(batch_df, batch_id):
    """Runs once per micro-batch. Writes every reading to latest_vitals,
    and anomalous readings to alerts."""
    rows = batch_df.collect()  # small batches — fine at this scale
    if not rows:
        return

    client = MongoClient(MONGO_URI)
    db = client["health_anomaly"]

    for row in rows:
        doc = row.asDict()

        # always update the latest reading for this user
        db.latest_vitals.update_one(
            {"user_id": doc["user_id"]},
            {"$set": doc},
            upsert=True
        )

        # only write to alerts if the rule actually fired
        if doc["detected_anomaly"]:
            alert_doc = {
                "_id": f"{doc['user_id']}|{doc['timestamp']}",  # idempotent — reruns upsert, don't duplicate
                "user_id": doc["user_id"],
                "timestamp": doc["timestamp"],
                "heart_rate": doc["heart_rate"],
                "body_temp_c": doc["body_temp_c"],
                "spo2": doc["spo2"],
                "rule": "threshold",
                "is_injected_anomaly": doc["is_injected_anomaly"],
            }
            db.alerts.replace_one(
                {"_id": alert_doc["_id"]},
                alert_doc,
                upsert=True
            )

    client.close()
    print(f"Batch {batch_id}: wrote {len(rows)} readings to Mongo")


query = scored.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("update") \
    .start()

query.awaitTermination()
