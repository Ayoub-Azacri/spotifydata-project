"""
Spark Job : streaming_enrichment_job
====================================
Consomme les topics `listening_events` et `p2p_network_events`, enrichit les écoutes
avec le catalogue PostgreSQL, effectue une jointure stream-stream et écrit les résultats.

Outputs :
    - Kafka → topic `enriched_events`
    - MinIO → bucket `spotify-parquet` sous /enriched (partitionné par date/hour)

Lancement :
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        spark_jobs/streaming_enrichment_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL", "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":   "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

# Checkpoint paths on MinIO
CHECKPOINT_MINIO = "s3a://spotify-checkpoints/enriched_minio"
CHECKPOINT_KAFKA = "s3a://spotify-checkpoints/enriched_kafka"

# ─────────────────────────────────────────────────────────────
# SCHÉMAS
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("track_id",    StringType(),    False),
    StructField("source_peer", StringType(),    True),
    StructField("timestamp",   StringType(),    False),
    StructField("duration_ms", IntegerType(),   True),
    StructField("device_type", StringType(),    True),
    StructField("geo_country", StringType(),    True),
    StructField("completed",   BooleanType(),   True),
    StructField("event_source",StringType(),    True),
])

P2P_NETWORK_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("event_type",  StringType(),    False),
    StructField("peer_id",     StringType(),    False),
    StructField("timestamp",   StringType(),    False),
    StructField("target_peer", StringType(),    True),
    StructField("chunk_id",    StringType(),    True),
    StructField("size_bytes",  IntegerType(),   True),
    StructField("track_id",    StringType(),    True),
])

# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-enrichment")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.fs.s3a.endpoint",             "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.impl",                 "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

# ─────────────────────────────────────────────────────────────
# LECTURE STREAMS
# ─────────────────────────────────────────────────────────────

def read_listening_events(spark: SparkSession):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "listening_events")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.isolation.level", "read_committed")
        .load()
    )
    return (
        raw
        .select(F.from_json(F.col("value").cast("string"), LISTENING_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
    )

def read_p2p_network_events(spark: SparkSession):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "p2p_network_events")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.isolation.level", "read_committed")
        .load()
    )
    return (
        raw
        .select(F.from_json(F.col("value").cast("string"), P2P_NETWORK_EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("network_time", F.to_timestamp(F.col("timestamp")))
    )

# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage du job streaming_enrichment_job...")

    # 1. Chargement du catalogue statique PostgreSQL (jointure stream-static)
    tracks_df = spark.read.jdbc(POSTGRES_URL, "tracks", properties=POSTGRES_PROPS)
    artists_df = spark.read.jdbc(POSTGRES_URL, "artists", properties=POSTGRES_PROPS)
    
    catalog_df = tracks_df.join(artists_df, tracks_df.artist_id == artists_df.id, "inner") \
        .select(
            tracks_df.id.alias("catalog_track_id"),
            tracks_df.title.alias("track_title"),
            tracks_df.genre.alias("genre"),
            artists_df.name.alias("artist")
        )

    # 2. Lecture des streams Kafka
    listening_df = read_listening_events(spark)
    network_df = read_p2p_network_events(spark)

    # 3. Jointure Stream-Static (Listening Events × Catalogue)
    enriched_static = listening_df.join(
        catalog_df,
        listening_df.track_id == catalog_df.catalog_track_id,
        "left"
    )

    # 4. Jointure Stream-Stream (Listening × P2P Network Events) avec Watermark de 2 minutes
    enriched_static_watermarked = enriched_static.withWatermark("event_time", "2 minutes")
    network_df_watermarked = network_df.withWatermark("network_time", "2 minutes")

    network_select = network_df_watermarked.select(
        F.col("event_id").alias("net_event_id"),
        F.col("peer_id").alias("net_peer_id"),
        F.col("event_type").alias("net_event_type"),
        F.col("target_peer").alias("net_target_peer"),
        F.col("chunk_id").alias("net_chunk_id"),
        F.col("size_bytes").alias("net_size_bytes"),
        F.col("network_time")
    )

    joined_stream = enriched_static_watermarked.join(
        network_select,
        F.expr("""
            source_peer = net_peer_id AND
            network_time >= event_time - interval 2 minutes AND
            network_time <= event_time + interval 2 minutes
        """),
        "left"
    )

    # 5. Déduplication par event_id
    deduplicated = joined_stream.dropDuplicates(["event_id", "event_time"])

    # 6. Structurer l'output final
    final_df = deduplicated.select(
        F.col("event_id"),
        F.col("user_id"),
        F.col("track_id"),
        F.col("track_title"),
        F.col("artist"),
        F.col("genre"),
        F.col("source_peer"),
        F.col("net_target_peer").alias("target_peer"),
        F.col("net_chunk_id").alias("chunk_id"),
        F.col("net_size_bytes").alias("size_bytes"),
        F.col("timestamp"),
        F.col("event_time"),
        F.col("duration_ms"),
        F.col("device_type"),
        F.col("geo_country"),
        F.col("completed"),
        F.col("event_source")
    )

    # Préparer le format de stockage MinIO (Parquet avec partition date/hour)
    minio_df = final_df.withColumn("date", F.date_format(F.col("event_time"), "yyyy-MM-dd")) \
                        .withColumn("hour", F.date_format(F.col("event_time"), "HH"))

    # 7. Écritures
    
    # Sink 1 : MinIO (Parquet)
    query_minio = (
        minio_df.writeStream
        .format("parquet")
        .option("checkpointLocation", CHECKPOINT_MINIO)
        .partitionBy("date", "hour")
        .start("s3a://spotify-parquet/enriched")
    )

    # Sink 2 : Kafka enriched_events
    query_kafka = (
        final_df
        .select(F.to_json(F.struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "enriched_events")
        .option("checkpointLocation", CHECKPOINT_KAFKA)
        .start()
    )

    # Attendre la fin
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
