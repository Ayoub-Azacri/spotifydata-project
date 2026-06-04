"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis (LIST),
les valide, les enrichit avec le catalogue et les stocke.

Planification : toutes les 5 minutes
Catchup       : désactivé (micro-batch temps réel)
"""

import json
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import redis
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis LIST,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis list `list:listening_events`
- Redis list `list:p2p_network_events`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides)

### Idempotence
Chaque event est identifié par `event_id` (UUID). L'upsert utilise
`ON CONFLICT (id) DO NOTHING` pour éviter les doublons.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = os.environ.get("REDIS_URL", "redis://redis:6379/1")
MINIO_BUCKET     = "spotify-parquet"

with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        """Consomme les événements depuis les listes Redis (pattern RPOP)."""
        r = redis.from_url(REDIS_URL, decode_responses=True)
        events = {"listening": [], "p2p_network": []}
        
        # Consommer listening_events
        while True:
            msg = r.rpop("list:listening_events")
            if not msg:
                break
            events["listening"].append(json.loads(msg))
            
        # Consommer p2p_network_events
        while True:
            msg = r.rpop("list:p2p_network_events")
            if not msg:
                break
            events["p2p_network"].append(json.loads(msg))
            
        logger.info("Consommé %d listening events et %d p2p events", 
                    len(events["listening"]), len(events["p2p_network"]))
        return events

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """Valide les événements et isole les invalides en DLQ."""
        from src.transformations.events import is_valid_listening_event, is_valid_p2p_event
        
        valid_listening = []
        valid_p2p = []
        dlq_entries = []
        
        for event in raw_events["listening"]:
            if is_valid_listening_event(event):
                valid_listening.append(event)
            else:
                dlq_entries.append({"payload": event, "error": "validation", "topic": "listening_events"})
                
        for event in raw_events["p2p_network"]:
            if is_valid_p2p_event(event):
                valid_p2p.append(event)
            else:
                dlq_entries.append({"payload": event, "error": "validation", "topic": "p2p_network_events"})
                
        # Flush DLQ
        if dlq_entries:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cur = conn.cursor()
            for entry in dlq_entries:
                cur.execute(
                    "INSERT INTO dead_letter_events (original_topic, payload, error_type, status) VALUES (%s, %s, %s, 'pending')",
                    (entry["topic"], json.dumps(entry["payload"]), entry["error"])
                )
            conn.commit()
            cur.close()
            conn.close()
            
        return {
            "valid_listening": valid_listening,
            "valid_p2p":       valid_p2p
        }

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """Enrichit les événements d'écoute avec les données du catalogue."""
        listening_events = validated["valid_listening"]
        if not listening_events:
            return []
            
        track_ids = list(set(e["track_id"] for e in listening_events))
        
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        # On récupère le titre et l'id artiste pour enrichir l'événement
        df_tracks = hook.get_pandas_df(
            "SELECT id as track_id, title as track_title FROM tracks WHERE id = ANY(%s::uuid[])",
            parameters=(track_ids,)
        )
        
        # Jointure via Pandas
        df_events = pd.DataFrame(listening_events)
        df_enriched = df_events.merge(df_tracks, on="track_id", how="left")
            
        return df_enriched.to_dict(orient="records")

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """Sauvegarde les événements enrichis en Parquet sur MinIO."""
        if not enriched_events:
            return "empty"
            
        df = pd.DataFrame(enriched_events)
        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        
        run_id = context["run_id"]
        filename = f"part-{run_id}.parquet"
        local_path = f"/tmp/{filename}"
        s3_path = f"listening_events/date={date_str}/hour={hour_str}/{filename}"
        
        df.to_parquet(local_path, index=False)
        
        s3 = S3Hook(aws_conn_id="spotify_minio")
        s3.load_file(local_path, key=s3_path, bucket_name=MINIO_BUCKET, replace=True)
        
        os.remove(local_path)
        return s3_path

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context):
        """Insère les événements dans PostgreSQL."""
        if not enriched_events:
            return
            
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        for e in enriched_events:
            try:
                cur.execute(
                    """
                    INSERT INTO listening_events (id, user_id, track_id, source_peer_id, timestamp, duration_ms, device_type, geo_country, completed, event_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        e["event_id"], e["user_id"], e["track_id"], e.get("source_peer"),
                        e["timestamp"], e["duration_ms"], e.get("device_type"),
                        e.get("geo_country"), e.get("completed", False), e.get("event_source")
                    )
                )
            except Exception as ex:
                logger.error("Erreur insertion event %s : %s", e["event_id"], ex)
                
        conn.commit()
        cur.close()
        conn.close()

    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)
    enriched  = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)
