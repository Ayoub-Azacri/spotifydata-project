"""
kafka_console_job.py
====================
Issue #13 — Simple Spark Structured Streaming: Kafka → Console

Reads `listening_events` topic, parses JSON, prints to console.

Launch:
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
        spark_jobs/kafka_console_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType,
)

# ─── CONFIG ───────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:9092")
KAFKA_TOPIC     = "listening_events"
CHECKPOINT_PATH = "s3a://spotify-checkpoints/kafka_console"

# ─── SCHEMA ───────────────────────────────────────────────────
LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",     StringType(),  False),
    StructField("user_id",      StringType(),  False),
    StructField("track_id",     StringType(),  False),
    StructField("source_peer",  StringType(),  True),
    StructField("timestamp",    StringType(),  False),
    StructField("duration_ms",  IntegerType(), True),
    StructField("device_type",  StringType(),  True),
    StructField("geo_country",  StringType(),  True),
    StructField("completed",    BooleanType(), True),
    StructField("event_source", StringType(),  True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SpotifyKafkaConsole")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        # MinIO / S3A (checkpoint)
        .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    """Read raw bytes from Kafka and parse JSON."""
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw
        .select(
            F.col("offset"),
            F.col("partition"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(
                F.col("value").cast("string"),
                LISTENING_EVENT_SCHEMA,
            ).alias("data"),
        )
        .select(
            "offset",
            "partition",
            "kafka_timestamp",
            "data.*",
        )
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("timestamp")),
        )
    )
    return parsed


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[kafka_console_job] Kafka: {KAFKA_BOOTSTRAP} → topic: {KAFKA_TOPIC}")

    events_df = read_kafka_stream(spark)

    query = (
        events_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
