# Save this as 'spark_processor.py'
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, udf, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
from textblob import TextBlob # TextBlob is easier for this use case

# UDF for sentiment analysis
def get_sentiment(text):
    if not text:
        return 0.0
    # Clean HTML tags
    clean_text = re.sub(r'<[^<]+?>', '', text)
    return TextBlob(clean_text).sentiment.polarity

sentiment_udf = udf(get_sentiment, DoubleType())

# Spark Session
spark = SparkSession.builder.appName("MastodonDataProcessor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Kafka configuration
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "mastodon-raw")
PROCESSED_TOPIC = os.getenv("KAFKA_PROCESSED_TOPIC", "mastodon-processed")

# Schema for raw messages
schema = StructType([
    StructField("user", StringType()),
    StructField("content", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("timestamp", StringType())
])

# Read from raw topic
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", RAW_TOPIC) \
    .load()

# Parse JSON and add sentiment
processed_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("sentiment_polarity", sentiment_udf(col("content")))

# Explode hashtags and select final columns for output
exploded_df = processed_df.withColumn("hashtag", explode(col("hashtags"))) \
    .select(
        col("user"),
        col("content"),
        col("timestamp"),
        col("hashtag"),
        col("sentiment_polarity")
    )

# Write to processed topic
query = exploded_df.select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("topic", PROCESSED_TOPIC) \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

query.awaitTermination()
