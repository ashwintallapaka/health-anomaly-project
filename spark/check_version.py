from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print("CLUSTER SPARK VERSION:", spark.version)
spark.stop()
