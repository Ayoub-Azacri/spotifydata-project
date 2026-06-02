"""
DAG : dlq_reprocessing_pipeline
===============================
Retraite périodiquement les événements en erreur stockés dans la DLQ.
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Tente de retraiter périodiquement les événements qui ont échoué lors de la validation
ou de l'enrichissement et qui ont été envoyés dans la table `dead_letter_events`.

### Stratégie
- Sélectionne les événements avec `status = 'pending'`.
- Tente une ré-ingestion dans `listening_events`.
- En cas de succès : `status = 'reprocessed'`.
- En cas d'échec : incrémente `retry_count`.
- Après 3 tentatives : `status = 'abandoned'`.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
}

POSTGRES_CONN_ID = "spotify_postgres"

with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement de la Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    tags=["spotify", "phase-1", "dlq"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq() -> list:
        """Récupère les événements en attente de retraitement."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
            SELECT id, original_topic, payload, retry_count 
            FROM dead_letter_events 
            WHERE status = 'pending' AND retry_count < 3 
            LIMIT 100
        """
        df = hook.get_pandas_df(sql)
        if df.empty:
            logger.info("Aucun événement en attente dans la DLQ")
            return []
        return df.to_dict(orient="records")

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list) -> list:
        """Tente de retraiter chaque événement."""
        if not pending_events:
            return []
            
        from src.transformations.events import is_valid_listening_event
        
        results = []
        for event in pending_events:
            event_id = event["id"]
            topic = event["original_topic"]
            payload = event["payload"]
            
            # Si le payload est une string JSON, on le parse
            if isinstance(payload, str):
                try:
                    payload_dict = json.loads(payload)
                except json.JSONDecodeError:
                    payload_dict = {}
            else:
                payload_dict = payload
            
            success = False
            error = None
            
            # Logique de retraitement simplifiée :
            # Si c'était un listening_event, on tente de le réinjecter
            if topic == "listening_events":
                if is_valid_listening_event(payload_dict):
                    try:
                        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
                        sql = """
                            INSERT INTO listening_events (id, user_id, track_id, timestamp, duration_ms)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO NOTHING
                        """
                        hook.run(sql, parameters=(
                            payload_dict["event_id"], payload_dict["user_id"], payload_dict["track_id"], 
                            payload_dict["timestamp"], payload_dict["duration_ms"]
                        ))
                        success = True
                    except Exception as e:
                        error = str(e)
                else:
                    error = "Still invalid schema"
            else:
                error = f"Unsupported topic for automated recovery: {topic}"
                
            results.append({
                "id": event_id,
                "success": success,
                "error": error,
                "retry_count": event["retry_count"] + 1
            })
            
        return results

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: list):
        """Met à jour le statut des événements dans la DLQ."""
        if not results:
            return
            
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        reprocessed_count = 0
        abandoned_count = 0
        
        for res in results:
            if res["success"]:
                cur.execute(
                    "UPDATE dead_letter_events SET status = 'reprocessed', resolved_at = NOW(), retry_count = %s WHERE id = %s",
                    (res["retry_count"], res["id"])
                )
                reprocessed_count += 1
            elif res["retry_count"] >= 3:
                cur.execute(
                    "UPDATE dead_letter_events SET status = 'abandoned', retry_count = %s, error_message = %s WHERE id = %s",
                    (res["retry_count"], res["error"], res["id"])
                )
                abandoned_count += 1
            else:
                cur.execute(
                    "UPDATE dead_letter_events SET retry_count = %s, error_message = %s, last_retry_at = NOW() WHERE id = %s",
                    (res["retry_count"], res["error"], res["id"])
                )
        
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info("Bilan DLQ : %d retraités, %d abandonnés", reprocessed_count, abandoned_count)

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)
