"""
DAG : late_events_reprocess_pipeline
======================================
Lit les événements tardifs depuis MinIO (spotify-checkpoints/late/)
et les ré-agrège dans listening_aggregates via upsert PostgreSQL.

Architecture :
    scan_late_events_minio()      ← liste les Parquets des 2 dernières heures
        → download_and_validate()     ← télécharge + valide le schéma
            → re_aggregate_into_postgres()  ← upsert dans listening_aggregates
                → mark_processed()          ← déplace vers late/processed/
                    → cleanup_old_processed()  ← supprime fichiers > 7 jours

TODO :
    [ ] Ajouter une validation de schéma stricte avec Great Expectations
    [ ] Alerter si le volume d'événements tardifs dépasse un seuil
"""

import io
import logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## late_events_reprocess_pipeline

### Rôle
Détecte et réintègre les événements d'écoute arrivés en retard dans le pipeline.
Les fichiers Parquet sont lus depuis MinIO `spotify-checkpoints/late/`,
validés, puis fusionnés dans `listening_aggregates` par upsert idempotent.

### Stratégie
- Fenêtre de scan : **2 dernières heures** (horaire, glissant).
- Upsert idempotent : `INSERT ... ON CONFLICT (track_id, agg_date) DO UPDATE`.
- Fichiers traités déplacés vers `late/processed/` pour audit.
- Nettoyage automatique des fichiers `processed/` de plus de **7 jours**.

### Connexions requises
- `spotify_postgres` — PostgreSQL cible
- `spotify_minio`    — Stockage objet MinIO
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID    = "spotify_postgres"
MINIO_CONN_ID       = "spotify_minio"
LATE_BUCKET         = "spotify-checkpoints"
LATE_PREFIX         = "late/"
PROCESSED_PREFIX    = "late/processed/"
SCAN_WINDOW_HOURS   = 2
RETENTION_DAYS      = 7

REQUIRED_COLUMNS = {
    "track_id", "user_id", "timestamp", "duration_ms",
}


def _get_s3_client():
    """Instancie un client boto3 S3 compatible MinIO."""
    import boto3
    from airflow.hooks.base import BaseHook
    conn  = BaseHook.get_connection(MINIO_CONN_ID)
    extra = conn.extra_dejson
    host  = conn.host or "minio"
    port  = conn.port or 9000
    endpoint = f"http://{host}:{port}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=conn.login or "minioadmin",
        aws_secret_access_key=conn.password or "minioadmin",
    )


def _list_late_files(cutoff):
    """Liste les objets Parquet dans spotify-checkpoints/late/ plus récents que cutoff."""
    import boto3
    s3 = _get_s3_client()
    try:
        resp = s3.list_objects_v2(Bucket=LATE_BUCKET, Prefix=LATE_PREFIX)
    except Exception as e:
        logger.warning("MinIO scan error (bucket vide ?): %s", e)
        return []
    late_files = []
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if key.startswith(PROCESSED_PREFIX) or not key.endswith(".parquet"):
            continue
        if obj["LastModified"].replace(tzinfo=None) >= cutoff.replace(tzinfo=None):
            late_files.append(key)
    return late_files


with DAG(
    dag_id="late_events_reprocess_pipeline",
    default_args=DEFAULT_ARGS,
    description="Réintégration des événements tardifs MinIO → PostgreSQL",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-2", "late-events", "reprocessing"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="scan_late_events_minio")
    def scan_late_events_minio(**context) -> list:
        """
        Liste les fichiers Parquet dans spotify-checkpoints/late/
        modifiés au cours des SCAN_WINDOW_HOURS dernières heures.
        Retourne une liste de noms d'objets MinIO.
        """
        cutoff     = datetime.now(tz=timezone.utc) - timedelta(hours=SCAN_WINDOW_HOURS)
        late_files = _list_late_files(cutoff)

        logger.info(
            "scan_late_events_minio: %d fichier(s) trouvé(s) dans la fenêtre [%s, now]",
            len(late_files), cutoff.isoformat(),
        )
        return late_files

    @task(task_id="download_and_validate")
    def download_and_validate(late_files: list) -> list:
        """
        Télécharge chaque fichier Parquet et valide la présence des colonnes requises.
        Retourne la liste des fichiers valides (nom d'objet).
        Logue un WARNING pour chaque fichier invalide (colonnes manquantes).
        """
        import pandas as pd

        if not late_files:
            logger.info("Aucun fichier tardif à valider.")
            return []

        client      = _get_s3_client()
        valid_files = []

        for object_name in late_files:
            try:
                response = client.get_object(Bucket=LATE_BUCKET, Key=object_name)
                raw      = response["Body"].read()

                df      = pd.read_parquet(io.BytesIO(raw))
                missing = REQUIRED_COLUMNS - set(df.columns)

                if missing:
                    logger.warning(
                        "⚠️  Schéma invalide pour '%s' — colonnes manquantes : %s",
                        object_name, missing,
                    )
                    continue

                if df.empty:
                    logger.warning("⚠️  Fichier vide ignoré : %s", object_name)
                    continue

                valid_files.append(object_name)
                logger.info("✅ Validé : %s (%d lignes)", object_name, len(df))

            except Exception as exc:
                logger.error("❌ Erreur lecture '%s' : %s", object_name, exc)

        logger.info(
            "download_and_validate: %d/%d fichiers valides",
            len(valid_files), len(late_files),
        )
        return valid_files

    @task(task_id="re_aggregate_into_postgres")
    def re_aggregate_into_postgres(valid_files: list) -> list:
        """
        Pour chaque fichier valide :
          1. Télécharge le Parquet.
          2. Calcule les agrégats (track_id, agg_date, total_streams, unique_listeners, total_duration_ms).
          3. Upsert idempotent dans daily_streams.
        Retourne la liste des fichiers traités avec succès.
        """
        import pandas as pd

        if not valid_files:
            logger.info("Aucun fichier à réintégrer.")
            return []

        client         = _get_s3_client()
        hook           = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn           = hook.get_conn()
        cur            = conn.cursor()
        processed_ok   = []
        total_upserted = 0

        try:
            for object_name in valid_files:
                try:
                    response = client.get_object(Bucket=LATE_BUCKET, Key=object_name)
                    raw      = response["Body"].read()

                    df              = pd.read_parquet(io.BytesIO(raw))
                    df["agg_date"]  = pd.to_datetime(df["timestamp"]).dt.date
                    agg             = (
                        df.groupby(["track_id", "agg_date"])
                        .agg(
                            total_streams    =("track_id",    "count"),
                            unique_listeners =("user_id",     "nunique"),
                            total_duration_ms=("duration_ms", "sum"),
                            countries        =("geo_country", lambda x: list(set(x.dropna())))
                        )
                        .reset_index()
                    )

                    for _, row in agg.iterrows():
                        cur.execute(
                            """
                            INSERT INTO daily_streams
                                (track_id, date, total_streams, unique_listeners, total_duration_ms, countries, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (track_id, date) DO UPDATE SET
                                total_streams     = daily_streams.total_streams     + EXCLUDED.total_streams,
                                unique_listeners  = GREATEST(daily_streams.unique_listeners, EXCLUDED.unique_listeners),
                                total_duration_ms = daily_streams.total_duration_ms + EXCLUDED.total_duration_ms,
                                countries         = ARRAY(SELECT DISTINCT unnest(daily_streams.countries || EXCLUDED.countries)),
                                updated_at        = NOW()
                            """,
                            (
                                str(row["track_id"]),
                                row["agg_date"],
                                int(row["total_streams"]),
                                int(row["unique_listeners"]),
                                float(row["total_duration_ms"]),
                                list(row["countries"]),
                            ),
                        )
                        total_upserted += 1

                    processed_ok.append(object_name)
                    logger.info("✅ Réintégré : %s (%d agrégats)", object_name, len(agg))

                except Exception as exc:
                    logger.error("❌ Erreur réintégration '%s' : %s", object_name, exc)

            conn.commit()
            logger.info(
                "re_aggregate_into_postgres: %d fichiers traités, %d upserts",
                len(processed_ok), total_upserted,
            )

        except Exception as exc:
            conn.rollback()
            logger.error("❌ Rollback global : %s", exc)
            raise
        finally:
            cur.close()
            conn.close()

        return processed_ok

    @task(task_id="mark_processed")
    def mark_processed(processed_files: list) -> list:
        """
        Déplace chaque fichier traité de late/ vers late/processed/
        en effectuant un copy_object suivi d'un remove_object.
        Retourne la liste des objets destination (pour audit).
        """
        if not processed_files:
            logger.info("Aucun fichier à déplacer.")
            return []

        client   = _get_s3_client()
        moved    = []

        for object_name in processed_files:
            filename    = object_name.split("/")[-1]
            dest_name   = f"{PROCESSED_PREFIX}{filename}"

            try:
                client.copy_object(
                    Bucket=LATE_BUCKET,
                    CopySource={'Bucket': LATE_BUCKET, 'Key': object_name},
                    Key=dest_name,
                )
                client.delete_object(Bucket=LATE_BUCKET, Key=object_name)
                moved.append(dest_name)
                logger.info("📦 Déplacé : %s → %s", object_name, dest_name)

            except Exception as exc:
                logger.error("❌ Erreur déplacement '%s' : %s", object_name, exc)

        logger.info("mark_processed: %d/%d fichiers déplacés", len(moved), len(processed_files))
        return moved

    @task(task_id="cleanup_old_processed")
    def cleanup_old_processed(**context):
        """
        Supprime les fichiers dans late/processed/ dont la date de modification
        est supérieure à RETENTION_DAYS jours.
        """
        client   = _get_s3_client()
        cutoff   = datetime.now(tz=timezone.utc) - timedelta(days=RETENTION_DAYS)
        try:
            resp = client.list_objects_v2(Bucket=LATE_BUCKET, Prefix=PROCESSED_PREFIX)
        except Exception as e:
            logger.warning("MinIO cleanup error (bucket vide ?): %s", e)
            return

        deleted  = 0
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            last_modified = obj["LastModified"]
            if last_modified and last_modified.replace(tzinfo=timezone.utc) < cutoff:
                try:
                    client.delete_object(Bucket=LATE_BUCKET, Key=key)
                    deleted += 1
                    logger.info("🗑️  Supprimé (>%dd) : %s", RETENTION_DAYS, key)
                except Exception as exc:
                    logger.error("❌ Erreur suppression '%s' : %s", key, exc)

        logger.info("cleanup_old_processed: %d fichier(s) supprimé(s)", deleted)

    # ── Orchestration ─────────────────────────────────────────
    late_files      = scan_late_events_minio()
    valid_files     = download_and_validate(late_files)
    processed_files = re_aggregate_into_postgres(valid_files)
    moved_files     = mark_processed(processed_files)
    cleanup_old_processed()
