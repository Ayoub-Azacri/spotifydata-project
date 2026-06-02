"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()        ← normalisation, dédoublonnage
        → load_to_postgres()         ← upsert avec ON CONFLICT
        → notify_success()
"""

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG
# ─────────────────────────────────────────────────────────────

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists` (upsert)
- Table `albums` (upsert)
- Table `tracks` (upsert)

### Idempotence
Le pipeline est idempotent : relancer plusieurs fois le même DAGrun
produit le même résultat grâce aux upserts ON CONFLICT DO UPDATE.

### Gestion des erreurs
- Schéma invalide → événement en DLQ (`dead_letter_events`)
- MinIO indisponible → retry x3 avec backoff exponentiel

### Monitoring
- XCom `tracks_inserted` : nombre de tracks insérées/mises à jour
- XCom `errors_count` : nombre d'entrées envoyées en DLQ
"""

# ─────────────────────────────────────────────────────────────
# CONFIGURATION PAR DÉFAUT
# ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":                 "spotify-team",
    "depends_on_past":       False,
    "start_date":            datetime(2025, 1, 1),
    "email_on_failure":      False,
    "email_on_retry":        False,
    "retries":               3,
    "retry_delay":           timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":     timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]


def _get_minio_client():
    """Crée un client boto3 configuré pour MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        region_name="us-east-1",
    )


def _insert_dlq(cursor, entries: list[dict], error_type: str, topic: str = "catalog_ingestion"):
    """Insère des entrées invalides dans la dead_letter_events."""
    if not entries:
        return
    for entry in entries:
        cursor.execute(
            """
            INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message, status)
            VALUES (%s, %s, %s, %s, 'pending')
            """,
            (topic, json.dumps(entry["payload"]), error_type, entry["error_message"]),
        )


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list[dict]:
        """
        Télécharge les fichiers JSON des labels depuis MinIO.
        Si un fichier est manquant, log un warning et continue.
        """
        s3 = _get_minio_client()
        catalogs = []

        for filename in LABEL_FILES:
            try:
                response = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                body = response["Body"].read().decode("utf-8")
                catalog = json.loads(body)
                catalogs.append(catalog)
                logger.info("✅ Fichier %s chargé — %d artists, %d tracks",
                            filename,
                            len(catalog.get("artists", [])),
                            len(catalog.get("tracks", [])))
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    logger.warning("⚠️ Fichier %s introuvable dans MinIO, skip", filename)
                else:
                    raise

        if not catalogs:
            raise ValueError("Aucun catalogue trouvé dans MinIO — abandon du DAGrun")

        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """
        Valide les champs obligatoires de chaque entité.
        Entrées invalides → dead_letter_events.
        """
        REQUIRED_ARTIST = {"id", "name", "label"}
        REQUIRED_ALBUM  = {"id", "artist_id", "title"}
        REQUIRED_TRACK  = {"id", "artist_id", "title", "duration_ms"}

        valid_artists = []
        valid_albums  = []
        valid_tracks  = []
        dlq_entries   = []

        for catalog in raw_catalogs:
            label = catalog.get("label", "unknown")

            # — Artists —
            for artist in catalog.get("artists", []):
                missing = REQUIRED_ARTIST - set(artist.keys())
                if missing:
                    dlq_entries.append({
                        "payload": artist,
                        "error_message": f"[{label}] Artist missing fields: {missing}",
                    })
                else:
                    valid_artists.append(artist)

            # — Albums —
            for album in catalog.get("albums", []):
                missing = REQUIRED_ALBUM - set(album.keys())
                if missing:
                    dlq_entries.append({
                        "payload": album,
                        "error_message": f"[{label}] Album missing fields: {missing}",
                    })
                else:
                    valid_albums.append(album)

            # — Tracks —
            for track in catalog.get("tracks", []):
                missing = REQUIRED_TRACK - set(track.keys())
                if missing:
                    dlq_entries.append({
                        "payload": track,
                        "error_message": f"[{label}] Track missing fields: {missing}",
                    })
                else:
                    valid_tracks.append(track)

        # Flush DLQ entries to PostgreSQL
        if dlq_entries:
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cur = conn.cursor()
            _insert_dlq(cur, dlq_entries, error_type="schema_validation")
            conn.commit()
            cur.close()
            conn.close()

        logger.info("Validation: %d artists, %d albums, %d tracks OK — %d en DLQ",
                     len(valid_artists), len(valid_albums), len(valid_tracks), len(dlq_entries))

        return {
            "valid": {
                "artists": valid_artists,
                "albums":  valid_albums,
                "tracks":  valid_tracks,
            },
            "errors_count": len(dlq_entries),
        }

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Normalise les noms d'artistes, déduplique, valide les durées.
        Utilise les fonctions de src/transformations/catalog.py.
        """
        from transformations.catalog import (
            normalize_artist_name,
            validate_track_schema,
            deduplicate_artists,
            deduplicate_tracks,
        )

        data = validated["valid"]

        # 1. Normaliser et dédupliquer les artistes
        artists = deduplicate_artists(data["artists"])
        logger.info("Artists après dédup : %d (avant : %d)", len(artists), len(data["artists"]))

        # 2. Valider et filtrer les tracks
        valid_tracks = []
        for track in data["tracks"]:
            errors = validate_track_schema(track)
            if errors:
                logger.warning("Track %s rejetée : %s", track.get("id"), errors)
            else:
                valid_tracks.append(track)

        tracks = deduplicate_tracks(valid_tracks)
        logger.info("Tracks après validation + dédup : %d (avant : %d)",
                     len(tracks), len(data["tracks"]))

        # 3. Albums — pas de transformation spécifique, juste dédup par id
        seen_album_ids = set()
        albums = []
        for album in data["albums"]:
            if album["id"] not in seen_album_ids:
                seen_album_ids.add(album["id"])
                albums.append(album)

        return {
            "artists": artists,
            "albums":  albums,
            "tracks":  tracks,
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Upsert idempotent dans PostgreSQL.
        ON CONFLICT → DO UPDATE pour garantir l'idempotence.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        stats = {"artists_inserted": 0, "albums_inserted": 0, "tracks_inserted": 0, "errors_count": 0}

        try:
            # ── Artists ──────────────────────────────────────
            for artist in transformed["artists"]:
                cur.execute(
                    """
                    INSERT INTO artists (id, name, country, label, genres, monthly_listeners, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (name, label) DO UPDATE SET
                        country = EXCLUDED.country,
                        genres = EXCLUDED.genres,
                        monthly_listeners = EXCLUDED.monthly_listeners,
                        updated_at = NOW()
                    """,
                    (
                        artist["id"],
                        artist["name"],
                        artist.get("country"),
                        artist.get("label"),
                        artist.get("genres", []),
                        artist.get("monthly_listeners", 0),
                    ),
                )
                stats["artists_inserted"] += 1

            # ── Albums ───────────────────────────────────────
            for album in transformed["albums"]:
                cur.execute(
                    """
                    INSERT INTO albums (id, artist_id, title, release_year, total_tracks, created_at)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        release_year = EXCLUDED.release_year,
                        total_tracks = EXCLUDED.total_tracks
                    """,
                    (
                        album["id"],
                        album["artist_id"],
                        album["title"],
                        album.get("release_year"),
                        album.get("total_tracks"),
                    ),
                )
                stats["albums_inserted"] += 1

            # ── Tracks ───────────────────────────────────────
            for track in transformed["tracks"]:
                cur.execute(
                    """
                    INSERT INTO tracks (id, album_id, artist_id, title, duration_ms, genre, bpm, explicit, audio_file_path, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        title = EXCLUDED.title,
                        duration_ms = EXCLUDED.duration_ms,
                        genre = EXCLUDED.genre,
                        bpm = EXCLUDED.bpm,
                        explicit = EXCLUDED.explicit,
                        updated_at = NOW()
                    """,
                    (
                        track["id"],
                        track.get("album_id"),
                        track["artist_id"],
                        track["title"],
                        track["duration_ms"],
                        track.get("genre"),
                        track.get("bpm"),
                        track.get("explicit", False),
                        track.get("audio_file_path"),
                    ),
                )
                stats["tracks_inserted"] += 1

            conn.commit()
            logger.info("✅ Load terminé : %s", stats)

        except Exception as e:
            conn.rollback()
            logger.error("❌ Erreur lors du chargement : %s", e)
            raise
        finally:
            cur.close()
            conn.close()

        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """Log de succès avec statistiques d'ingestion."""
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        ──────────────────────────────────────
        DAGRun          : {dag_run.run_id}
        Artists insérés  : {stats.get('artists_inserted', 0)}
        Albums insérés   : {stats.get('albums_inserted', 0)}
        Tracks insérées  : {stats.get('tracks_inserted', 0)}
        Erreurs DLQ      : {stats.get('errors_count', 0)}
        """)

    # ── Orchestration des tâches ──────────────────────────────
    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)
