"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs :
    - PostgreSQL → table `realtime_top_tracks` (top 10 par fenêtre de 5 min)
    - Redis      → clé `top_tracks:live` (top genres par sliding window)

Lancement :
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        spark_jobs/streaming_trends_job.py

TODO :
    [ ] Implémenter la lecture du topic Kafka avec readStream
    [ ] Désérialiser les messages JSON avec le bon schéma
    [ ] Implémenter les fenêtres tumbling de 5 minutes
    [ ] Implémenter les sliding windows pour les genres (15 min / 5 min)
    [ ] Configurer le checkpoint sur MinIO
    [ ] Écrire les résultats dans PostgreSQL et Redis
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = "s3a://spotify-checkpoints/streaming_trends"
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":   "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("track_id",    StringType(),    False),
    StructField("source_peer", StringType(),    True),
    StructField("timestamp",   StringType(),    False),  # ISO 8601 → à caster en Timestamp
    StructField("duration_ms", IntegerType(),   True),
    StructField("device_type", StringType(),    True),
    StructField("geo_country", StringType(),    True),
    StructField("completed",   BooleanType(),   True),
    StructField("event_source",StringType(),    True),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Crée et configure la SparkSession avec les dépendances nécessaires.

    TODO : vérifier que les packages kafka et postgresql sont disponibles
    """
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",             "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka `listening_events` en streaming.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("kafka.isolation.level", "read_committed")
        .load()
    )

    parsed = (
        raw
        .select(
            F.from_json(
                F.col("value").cast("string"),
                LISTENING_EVENT_SCHEMA,
            ).alias("data")
        )
        .select("data.*")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
    )
    return parsed


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top 10 des tracks par tumbling window de 5 minutes.
    """
    windowed = (
        events_df
        .groupBy(
            F.window("event_time", "5 minutes"),
            "track_id"
        )
        .agg(
            F.count("*").alias("stream_count"),
            F.approx_count_distinct("user_id").alias("unique_listeners")
        )
    )

    def write_to_postgres(batch_df, batch_id):
        import psycopg2
        rows = batch_df.collect()
        if not rows:
            return
            
        from pyspark.sql.window import Window
        window_spec = Window.partitionBy("window").orderBy(F.desc("stream_count"))
        
        ranked_df = (
            batch_df
            .withColumn("rank", F.row_number().over(window_spec))
            .filter(F.col("rank") <= 10)
        )
        
        ranked_rows = ranked_df.collect()
        if not ranked_rows:
            return
            
        try:
            conn = psycopg2.connect("postgresql://spotify:spotify@postgres:5432/spotify")
            cur = conn.cursor()
            for row in ranked_rows:
                w_start = row.window.start
                w_end = row.window.end
                cur.execute("""
                    INSERT INTO realtime_top_tracks (window_start, window_end, track_id, stream_count, unique_listeners, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (window_start, track_id) DO UPDATE SET
                        stream_count = EXCLUDED.stream_count,
                        unique_listeners = EXCLUDED.unique_listeners,
                        updated_at = NOW()
                """, (w_start, w_end, row.track_id, row.stream_count, row.unique_listeners))
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            print(f"Error writing to Postgres: {e}")

    query = (
        windowed.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )
    return query


def compute_genre_listeners_sliding(events_df, catalog_df):
    """
    Listeners uniques par genre en sliding window (15 min glissant toutes les 5 min).
    """
    # Join stream-static sur track_id
    joined = events_df.join(catalog_df, events_df.track_id == catalog_df.id, "inner")
    
    windowed = (
        joined
        .groupBy(
            F.window("event_time", "15 minutes", "5 minutes"),
            "genre"
        )
        .agg(
            F.approx_count_distinct("user_id").alias("unique_listeners")
        )
    )

    def write_to_redis(batch_df, batch_id):
        rows = batch_df.collect()
        if not rows:
            return
            
        import redis
        try:
            r = redis.Redis(host="redis", port=6379, db=1)
            genre_mapping = {}
            for row in rows:
                if row.genre:
                    genre_mapping[row.genre] = str(row.unique_listeners)
            if genre_mapping:
                pipe = r.pipeline()
                pipe.delete("genre_listeners:live")
                pipe.hset("genre_listeners:live", mapping=genre_mapping)
                pipe.execute()
        except Exception as e:
            print(f"Error writing to Redis: {e}")

    query = (
        windowed.writeStream
        .foreachBatch(write_to_redis)
        .outputMode("update")
        .option("checkpointLocation", "s3a://spotify-checkpoints/genre_listeners")
        .start()
    )
    return query


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage streaming_trends_job...")
    print(f"Kafka : {KAFKA_BOOTSTRAP} → topic : {KAFKA_TOPIC}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")

    # Lecture Kafka
    events_df = read_kafka_stream(spark)

    # 1. Router les late events (> 10 minutes) vers le topic dédié
    late_events = events_df.filter(
        F.col("event_time") < F.current_timestamp() - F.expr("INTERVAL 10 MINUTES")
    )
    query_late = (
        late_events
        .select(F.to_json(F.struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", "late_listening_events")
        .option("checkpointLocation", "s3a://spotify-checkpoints/late_events")
        .start()
    )

    # 2. Filtrer et appliquer le watermark (10 minutes) sur les events normaux
    normal_events = (
        events_df
        .filter(F.col("event_time") >= F.current_timestamp() - F.expr("INTERVAL 10 MINUTES"))
        .withWatermark("event_time", "10 minutes")
    )

    # Chargement du catalogue (jointure statique — Phase 2, seq 2.3)
    catalog_df = spark.read.jdbc(POSTGRES_URL, "tracks", properties=POSTGRES_PROPS)

    # 3. Exécuter les agrégations sur les normal_events watermarqués
    query_top_tracks = compute_top_tracks_tumbling(normal_events)
    query_genres     = compute_genre_listeners_sliding(normal_events, catalog_df)

    # Attendre l'arrêt gracieux
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
