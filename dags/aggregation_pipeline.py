"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (attend streaming_events_pipeline)
        → compute_top_tracks()      ← top 50 du jour → daily_streams
        → compute_artist_stats()    ← streams + unique_listeners → artist_stats
        → compute_p2p_metrics()     ← taux cache_hit, latence moyenne
        → update_aggregates()       ← écriture PostgreSQL

TODO :
    [ ] Configurer correctement l'ExternalTaskSensor
    [ ] Stratégie incrémentale : calculer uniquement pour la date d'exécution
    [ ] Ajouter doc_md sur ce DAG
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

logger = logging.getLogger(__name__)

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor.

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `execution_date` (le jour courant).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,     # attend la fin du DAGRun complet
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """
        Calcule le top 50 des tracks pour la date d'exécution.
        Requête incrémentale sur listening_events filtrée par date.
        """
        exec_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                track_id::text,
                COUNT(*)                        AS total_streams,
                COUNT(DISTINCT user_id)         AS unique_listeners,
                COALESCE(SUM(duration_ms), 0)   AS total_duration_ms,
                ARRAY_AGG(DISTINCT geo_country) FILTER (WHERE geo_country IS NOT NULL) AS countries
            FROM listening_events
            WHERE DATE(timestamp) = %s
              AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50
            """,
            (exec_date,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        result = [
            {
                "track_id":          row[0],
                "date":              str(exec_date),
                "total_streams":     row[1],
                "unique_listeners":  row[2],
                "total_duration_ms": row[3],
                "countries":         row[4] or [],
            }
            for row in rows
        ]

        logger.info("compute_top_tracks: %d tracks for %s", len(result), exec_date)
        return result

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """
        Calcule les statistiques par artiste pour la date d'exécution.
        Jointure listening_events x tracks pour obtenir artist_id.
        """
        exec_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                t.artist_id::text,
                COUNT(le.id)                AS total_streams,
                COUNT(DISTINCT le.user_id)  AS unique_listeners,
                (
                    SELECT le2.track_id::text
                    FROM listening_events le2
                    WHERE le2.track_id = ANY(
                        SELECT id FROM tracks WHERE artist_id = t.artist_id
                    )
                    AND DATE(le2.timestamp) = %s
                    AND le2.completed = TRUE
                    GROUP BY le2.track_id
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                ) AS top_track_id
            FROM listening_events le
            JOIN tracks t ON le.track_id = t.id
            WHERE DATE(le.timestamp) = %s
              AND le.completed = TRUE
            GROUP BY t.artist_id
            ORDER BY total_streams DESC
            """,
            (exec_date, exec_date),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        result = [
            {
                "artist_id":       row[0],
                "date":            str(exec_date),
                "total_streams":   row[1],
                "unique_listeners": row[2],
                "top_track_id":    row[3],
            }
            for row in rows
        ]

        logger.info("compute_artist_stats: %d artists for %s", len(result), exec_date)
        return result

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """
        Calcule les métriques du réseau P2P pour la date d'exécution.
        """
        exec_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE event_source = 'cache')::float
                    / NULLIF(COUNT(*), 0) AS cache_hit_rate,
                AVG(duration_ms)          AS avg_duration_ms,
                COUNT(DISTINCT source_peer_id) FILTER (WHERE source_peer_id IS NOT NULL) AS active_peers
            FROM listening_events
            WHERE DATE(timestamp) = %s
            """,
            (exec_date,),
        )
        row = cur.fetchone()

        cur.execute(
            """
            SELECT device_type, COUNT(*) as cnt
            FROM listening_events
            WHERE DATE(timestamp) = %s AND device_type IS NOT NULL
            GROUP BY device_type ORDER BY cnt DESC
            """,
            (exec_date,),
        )
        device_dist = {r[0]: r[1] for r in cur.fetchall()}

        cur.execute(
            """
            SELECT geo_country, COUNT(*) as cnt
            FROM listening_events
            WHERE DATE(timestamp) = %s AND geo_country IS NOT NULL
            GROUP BY geo_country ORDER BY cnt DESC LIMIT 10
            """,
            (exec_date,),
        )
        country_dist = {r[0]: r[1] for r in cur.fetchall()}

        cur.close()
        conn.close()

        metrics = {
            "date":                str(exec_date),
            "cache_hit_rate":      round(row[0] or 0.0, 4),
            "avg_duration_ms":     round(row[1] or 0.0, 2),
            "active_peers":        row[2] or 0,
            "device_distribution": device_dist,
            "country_distribution": country_dist,
        }

        logger.info("compute_p2p_metrics: cache_hit=%.2f%%, active_peers=%d",
                    metrics["cache_hit_rate"] * 100, metrics["active_peers"])
        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        """
        Écrit les agrégats dans PostgreSQL de façon idempotente.
        ON CONFLICT DO UPDATE sur (track_id, date) et (artist_id, date).
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            # ── daily_streams ─────────────────────────────────
            for row in top_tracks:
                cur.execute(
                    """
                    INSERT INTO daily_streams
                        (track_id, date, total_streams, unique_listeners, total_duration_ms, countries, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (track_id, date) DO UPDATE SET
                        total_streams     = EXCLUDED.total_streams,
                        unique_listeners  = EXCLUDED.unique_listeners,
                        total_duration_ms = EXCLUDED.total_duration_ms,
                        countries         = EXCLUDED.countries,
                        updated_at        = NOW()
                    """,
                    (
                        row["track_id"],
                        row["date"],
                        row["total_streams"],
                        row["unique_listeners"],
                        row["total_duration_ms"],
                        row["countries"],
                    ),
                )

            # ── artist_stats ──────────────────────────────────
            for row in artist_stats:
                cur.execute(
                    """
                    INSERT INTO artist_stats
                        (artist_id, date, total_streams, unique_listeners, top_track_id, updated_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (artist_id, date) DO UPDATE SET
                        total_streams    = EXCLUDED.total_streams,
                        unique_listeners = EXCLUDED.unique_listeners,
                        top_track_id     = EXCLUDED.top_track_id,
                        updated_at       = NOW()
                    """,
                    (
                        row["artist_id"],
                        row["date"],
                        row["total_streams"],
                        row["unique_listeners"],
                        row["top_track_id"],
                    ),
                )

            conn.commit()

            logger.info(
                "✅ update_aggregates done — %d top_tracks, %d artist_stats upserted | "
                "P2P: cache_hit=%.2f%%, peers=%d",
                len(top_tracks), len(artist_stats),
                p2p_metrics.get("cache_hit_rate", 0) * 100,
                p2p_metrics.get("active_peers", 0),
            )

        except Exception as e:
            conn.rollback()
            logger.error("❌ update_aggregates failed: %s", e)
            raise
        finally:
            cur.close()
            conn.close()

    # ── Orchestration ─────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
