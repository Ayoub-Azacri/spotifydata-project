"""
DAG : reconciliation_pipeline
==============================
Compare les agrégats batch vs streaming pour détecter les divergences de données.
Génère un rapport JSON et l'exporte dans MinIO.
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## reconciliation_pipeline

### Rôle
Compare les totaux de streams produits par le pipeline batch (`daily_streams`)
et le pipeline streaming (`realtime_top_tracks`) par track pour détecter les divergences.

### Seuil d'alerte
Un WARNING et un échec du DAG sont provoqués si le delta relatif dépasse **5 %** pour un track.

### Destinations
- Rapport JSON exporté dans MinIO : `spotify-parquet/reconciliation/YYYY-MM-DD.json`
- Rapport résumé inséré dans PostgreSQL : `reconciliation_reports`

### Stratégie
Incrémentale : calcule uniquement pour `data_interval_start` (le jour courant).
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
MINIO_CONN_ID    = "spotify_minio"
MINIO_BUCKET     = "spotify-parquet"
DELTA_THRESHOLD  = 0.05  # 5 %


with DAG(
    dag_id="reconciliation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Comparaison batch vs streaming aggregates pour la qualité des données",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-2", "reconciliation", "data-quality"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="get_batch_totals")
    def get_batch_totals(**context) -> dict:
        """
        Interroge daily_streams pour obtenir le total de streams par track pour la date d'exécution.
        Retourne un dict {track_id: total_streams}.
        """
        exec_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute(
            """
            SELECT
                track_id::text,
                COALESCE(total_streams, 0) AS total_streams
            FROM daily_streams
            WHERE date = %s
            """,
            (exec_date,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        totals = {row[0]: int(row[1]) for row in rows}
        logger.info("get_batch_totals: Loaded %d tracks for date %s", len(totals), exec_date)
        return totals

    @task(task_id="get_streaming_totals")
    def get_streaming_totals(**context) -> dict:
        """
        Interroge realtime_top_tracks pour obtenir le total de streams par track pour la date d'exécution.
        Retourne un dict {track_id: total_streams}.
        """
        exec_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        cur.execute(
            """
            SELECT
                track_id::text,
                COALESCE(SUM(stream_count), 0) AS total_streams
            FROM realtime_top_tracks
            WHERE DATE(window_start) = %s
            GROUP BY track_id
            """,
            (exec_date,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        totals = {row[0]: int(row[1]) for row in rows}
        logger.info("get_streaming_totals: Loaded %d tracks for date %s", len(totals), exec_date)
        return totals

    @task(task_id="compare_and_validate")
    def compare_and_validate(batch_totals: dict, streaming_totals: dict, **context) -> dict:
        """
        Compare les totaux batch vs streaming par track.
        Identifie les divergences dépassant DELTA_THRESHOLD (5 %).
        Retourne le rapport complet sous forme de dict.
        """
        exec_date = str(context["data_interval_start"].date())

        all_track_ids = set(batch_totals.keys()).union(set(streaming_totals.keys()))
        tracks_report = []
        total_divergent = 0
        max_delta_pct = 0.0

        for track_id in all_track_ids:
            batch_val = batch_totals.get(track_id, 0)
            stream_val = streaming_totals.get(track_id, 0)

            if batch_val == 0 and stream_val == 0:
                delta_abs = 0
                delta_pct = 0.0
            elif batch_val == 0:
                delta_abs = stream_val
                delta_pct = 1.0
            else:
                delta_abs = abs(batch_val - stream_val)
                delta_pct = delta_abs / batch_val

            divergent = delta_pct > DELTA_THRESHOLD
            if divergent:
                total_divergent += 1
            
            max_delta_pct = max(max_delta_pct, delta_pct)

            tracks_report.append({
                "track_id": track_id,
                "batch_count": batch_val,
                "streaming_count": stream_val,
                "delta_abs": delta_abs,
                "delta_pct": round(delta_pct * 100, 4),
                "divergent": divergent
            })

        status = "OK" if total_divergent == 0 else "DIVERGENCE"

        report = {
            "date":                   exec_date,
            "total_tracks_compared":  len(all_track_ids),
            "total_divergent_tracks": total_divergent,
            "max_divergence_pct":     round(max_delta_pct * 100, 4),
            "threshold_pct":          DELTA_THRESHOLD * 100,
            "status":                 status,
            "tracks":                 tracks_report,
            "generated_at":           datetime.utcnow().isoformat() + "Z",
        }

        if status == "DIVERGENCE":
            logger.warning(
                "⚠️ RECONCILIATION DIVERGENCE on %s — total_tracks=%d, divergent=%d, max_delta=%.2f%% (threshold=%.1f%%)",
                exec_date, len(all_track_ids), total_divergent, max_delta_pct * 100, DELTA_THRESHOLD * 100
            )
            for t in tracks_report:
                if t["divergent"]:
                    logger.warning("  - Track %s: batch=%d, streaming=%d, delta=%.2f%%", 
                                   t["track_id"], t["batch_count"], t["streaming_count"], t["delta_pct"])
        else:
            logger.info("✅ Reconciliation OK on %s — total_tracks=%d, max_delta=%.2f%%", 
                        exec_date, len(all_track_ids), max_delta_pct * 100)

        return report

    @task(task_id="export_report_to_sinks")
    def export_report_to_sinks(report: dict, **context):
        """
        Exporte le rapport dans MinIO et l'enregistre en base PostgreSQL.
        Lève une exception à la fin si une divergence de données a été constatée.
        """
        import boto3

        exec_date = report["date"]
        
        # 1. Export MinIO
        object_name = f"reconciliation/{exec_date}.json"
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        payload = json.dumps(report, indent=2, ensure_ascii=False).encode("utf-8")
        try:
            s3.put_object(Bucket=MINIO_BUCKET, Key=object_name, Body=payload, ContentType="application/json")
            logger.info("✅ Reconciliation report exported to MinIO → s3://%s/%s (%d bytes)",
                        MINIO_BUCKET, object_name, len(payload))
        except Exception as e:
            logger.warning("MinIO export failed (non-blocking): %s", e)

        # 2. Sauvegarde PostgreSQL
        try:
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            pg_hook.run(
                """
                INSERT INTO reconciliation_reports (reconciliation_date, total_tracks_compared, total_divergent_tracks, max_divergence_pct, report_json, created_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (reconciliation_date) DO UPDATE SET
                    total_tracks_compared = EXCLUDED.total_tracks_compared,
                    total_divergent_tracks = EXCLUDED.total_divergent_tracks,
                    max_divergence_pct = EXCLUDED.max_divergence_pct,
                    report_json = EXCLUDED.report_json,
                    created_at = NOW();
                """,
                parameters=(
                    exec_date,
                    report["total_tracks_compared"],
                    report["total_divergent_tracks"],
                    report["max_divergence_pct"],
                    json.dumps(report)
                )
            )
            logger.info("✅ Reconciliation report saved to PostgreSQL.")
        except Exception as e:
            logger.error("PostgreSQL save failed: %s", e)

        # 3. Alerte / Failure de la tâche
        if report["status"] == "DIVERGENCE":
            raise AirflowException(
                f"Reconciliation failure: {report['total_divergent_tracks']} track(s) have data divergence exceeding 5%!"
            )


    # ── Orchestration ─────────────────────────────────────────
    batch_totals     = get_batch_totals()
    streaming_totals = get_streaming_totals()
    report           = compare_and_validate(batch_totals, streaming_totals)
    export_report_to_sinks(report)
