"""
Spark Job : fraud_detection_job
=============================
Détecte les comportements frauduleux en temps réel à partir des topics :
- `listening_events` (détection de bot_stream)
- `p2p_network_events` (détection de free_rider)

Outputs :
- Kafka → topic `fraud_alerts`
- PostgreSQL → table `fraud_detections`
- PostgreSQL → table `dead_letter_events` (DLQ)

Lancement :
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        spark_jobs/fraud_detection_job.py
"""

import os
import json
from datetime import datetime, timezone
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, DoubleType, LongType, ArrayType, TimestampType
)
from pyspark.sql.streaming.state import GroupStateTimeout, GroupState

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

CHECKPOINT_USER_STATE = "s3a://spotify-checkpoints/fraud_user_state"
CHECKPOINT_P2P_STATE  = "s3a://spotify-checkpoints/fraud_p2p_state"

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
    StructField("status",      StringType(),    True),
])

# Schéma de l'état flatMapGroupsWithState
STATE_SCHEMA = StructType([
    # Liste de tuples (timestamp_ms, duration_ms)
    StructField("listens", ArrayType(
        StructType([
            StructField("timestamp_ms", LongType(), False),
            StructField("duration_ms", IntegerType(), False)
        ])
    ), True),
    StructField("suspicion_score", DoubleType(), False)
])

# Schéma d'output des alertes
ALERT_SCHEMA = StructType([
    StructField("user_id",         StringType(),    True),
    StructField("peer_id",         StringType(),    True),
    StructField("fraud_type",      StringType(),    False),
    StructField("suspicion_score", DoubleType(),    False),
    StructField("evidence",        StringType(),    False),  # JSON string
    StructField("window_start",    TimestampType(), True),
    StructField("window_end",      TimestampType(), True)
])

# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-fraud-detection")
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
# FONCTION STATEFUL (Règles 1 & 2)
# ─────────────────────────────────────────────────────────────

def update_user_state(key, pdf_iter, state: GroupState):
    user_id = key[0]
    
    # Récupérer l'état actuel ou l'initialiser
    if state.exists:
        curr_state = state.get
        listens = curr_state[0] if curr_state[0] is not None else []
        suspicion_score = curr_state[1]
    else:
        listens = []
        suspicion_score = 0.0

    # Ajouter les nouveaux événements
    new_listens = []
    for pdf in pdf_iter:
        for index, row in pdf.iterrows():
            if pd.notna(row["event_time"]):
                ts_ms = int(row["event_time"].timestamp() * 1000)
                new_listens.append((ts_ms, int(row["duration_ms"]) if pd.notna(row["duration_ms"]) else 0))

    listens.extend(new_listens)
    
    if not listens:
        return

    # Nettoyage des événements plus vieux d'une heure par rapport au plus récent
    latest_ts = max(l[0] for l in listens)
    one_hour_ago = latest_ts - 3600000
    listens = [l for l in listens if l[0] >= one_hour_ago]

    alerts_list = []

    # Règle 1 : Plus de 100 écoutes en 10 minutes (600000 ms)
    ten_mins_ago = latest_ts - 600000
    listens_10m = [l for l in listens if l[0] >= ten_mins_ago]
    if len(listens_10m) > 100:
        suspicion_score = min(1.0, suspicion_score + 0.5)
        evidence = {"streams_count_10m": len(listens_10m)}
        alerts_list.append({
            "user_id": user_id,
            "peer_id": None,
            "fraud_type": "burst_listen",
            "suspicion_score": suspicion_score,
            "evidence": json.dumps(evidence),
            "window_start": pd.Timestamp(ten_mins_ago, unit="ms", tz="UTC"),
            "window_end": pd.Timestamp(latest_ts, unit="ms", tz="UTC")
        })

    # Règle 2 : Durée moyenne < 5 secondes sur une fenêtre de 1 heure (min 5 écoutes)
    if len(listens) >= 5:
        avg_duration = sum(l[1] for l in listens) / len(listens)
        if avg_duration < 5000:  # < 5 secondes
            suspicion_score = min(1.0, suspicion_score + 0.6)
            evidence = {
                "avg_duration_1h_ms": avg_duration,
                "total_streams_1h": len(listens)
            }
            alerts_list.append({
                "user_id": user_id,
                "peer_id": None,
                "fraud_type": "bot_stream",
                "suspicion_score": suspicion_score,
                "evidence": json.dumps(evidence),
                "window_start": pd.Timestamp(one_hour_ago, unit="ms", tz="UTC"),
                "window_end": pd.Timestamp(latest_ts, unit="ms", tz="UTC")
            })

    # Sauvegarder l'état
    state.update((listens, suspicion_score))

    if alerts_list:
        yield pd.DataFrame(alerts_list)

# ─────────────────────────────────────────────────────────────
# ÉCRITURE DANS LES SINKS (foreachBatch)
# ─────────────────────────────────────────────────────────────

def write_alerts_to_sinks(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    import psycopg2
    from confluent_kafka import Producer

    # 1. Écrire dans PostgreSQL (fraud_detections et dead_letter_events)
    try:
        conn = psycopg2.connect("postgresql://spotify:spotify@postgres:5432/spotify")
        cur = conn.cursor()
        for row in rows:
            # 1.a Table fraud_detections
            cur.execute("""
                INSERT INTO fraud_detections (user_id, peer_id, fraud_type, suspicion_score, evidence, window_start, window_end, detected_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                row.user_id,
                row.peer_id,
                row.fraud_type,
                row.suspicion_score,
                row.evidence,
                row.window_start,
                row.window_end
            ))

            # 1.b Table dead_letter_events (DLQ)
            payload = {
                "user_id": row.user_id,
                "peer_id": row.peer_id,
                "fraud_type": row.fraud_type,
                "suspicion_score": row.suspicion_score,
                "evidence": json.loads(row.evidence) if row.evidence else {},
                "window_start": row.window_start.isoformat() if row.window_start else None,
                "window_end": row.window_end.isoformat() if row.window_end else None
            }
            cur.execute("""
                INSERT INTO dead_letter_events (original_topic, payload, error_type, status, created_at)
                VALUES ('fraud_alerts', %s, %s, 'pending', NOW())
            """, (
                json.dumps(payload),
                row.fraud_type
            ))
            
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error writing alerts to Postgres: {e}")

    # 2. Publier dans Kafka (topic fraud_alerts)
    try:
        producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all"
        })
        for row in rows:
            payload = {
                "user_id": row.user_id,
                "peer_id": row.peer_id,
                "fraud_type": row.fraud_type,
                "suspicion_score": row.suspicion_score,
                "evidence": json.loads(row.evidence) if row.evidence else {},
                "window_start": row.window_start.isoformat() if row.window_start else None,
                "window_end": row.window_end.isoformat() if row.window_end else None
            }
            key = row.user_id or row.peer_id or ""
            producer.produce("fraud_alerts", key=key, value=json.dumps(payload))
        producer.flush(timeout=5)
    except Exception as e:
        print(f"Error publishing alerts to Kafka: {e}")


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage du job fraud_detection_job...")

    # 1. Lecture des streams Kafka
    listening_stream = read_listening_events(spark)
    p2p_stream = read_p2p_network_events(spark)

    # 2. Application de flatMapGroupsWithState sur listening_stream (Règles 1 et 2)
    # Appliquer un watermark pour pouvoir utiliser les opérations stateful
    listening_watermarked = listening_stream.withWatermark("event_time", "1 hour")
    
    user_alerts = (
        listening_watermarked
        .groupBy("user_id")
        .applyInPandasWithState(
            func=update_user_state,
            outputStructType=ALERT_SCHEMA,
            stateStructType=STATE_SCHEMA,
            outputMode="Update",
            timeoutConf="NoTimeout"
        )
    )

    # 3. Application de la Règle 3 sur p2p_stream (Taux d'échec P2P > 50% sur 15 min sliding)
    p2p_transfers = p2p_stream.filter(F.col("event_type") == "chunk_transfer")
    p2p_watermarked = p2p_transfers.withWatermark("network_time", "15 minutes")

    peer_alerts = (
        p2p_watermarked
        .groupBy(
            F.window("network_time", "15 minutes", "5 minutes"),
            F.col("peer_id")
        )
        .agg(
            F.count("*").alias("total_transfers"),
            F.sum(F.when(F.col("status") == "failed", 1).otherwise(0)).alias("failed_transfers")
        )
        .filter((F.col("total_transfers") >= 5) & ((F.col("failed_transfers") / F.col("total_transfers")) > 0.5))
        .select(
            F.lit(None).cast("string").alias("user_id"),
            F.col("peer_id"),
            F.lit("free_rider").alias("fraud_type"),
            (F.col("failed_transfers") / F.col("total_transfers")).alias("suspicion_score"),
            F.to_json(F.struct("total_transfers", "failed_transfers")).alias("evidence"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end")
        )
    )

    # 4. Écritures
    query_users = (
        user_alerts.writeStream
        .foreachBatch(write_alerts_to_sinks)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_USER_STATE)
        .start()
    )

    query_peers = (
        peer_alerts.writeStream
        .foreachBatch(write_alerts_to_sinks)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_P2P_STATE)
        .start()
    )

    # Attendre l'arrêt gracieux
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
