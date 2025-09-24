from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType
from pyspark.sql.functions import col, explode, lower

spark = SparkSession.builder \
    .appName("MastodonHashtagConsumer") \
    .getOrCreate()

# Schema for our producer payload
schema = StructType([
    StructField("user", StringType(), True),
    StructField("content", StringType(), True),
    StructField("hashtags", ArrayType(StringType()), True),
    StructField("timestamp", StringType(), True)  # or TimestampType if you prefer
])

# Read raw Kafka topic
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mastodon-raw") \
    .load()

# Parse JSON
json_df = raw_df.selectExpr("CAST(value AS STRING) as json") \
    .selectExpr("from_json(json, '{}') as data".format(schema.json())) \
    .select("data.*")

# Explode hashtags
hashtags_df = json_df.withColumn("hashtag", explode("hashtags")) \
    .withColumn("hashtag", lower(col("hashtag")))

# Debug to console first (see if hashtags appear)
debug_query = hashtags_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

debug_query.awaitTermination()
