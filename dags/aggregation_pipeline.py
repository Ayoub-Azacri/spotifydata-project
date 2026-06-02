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
    [ ] Implémenter compute_top_tracks()
    [ ] Implémenter compute_artist_stats()
    [ ] Implémenter compute_p2p_metrics()
    [ ] Implémenter update_aggregates()
    [ ] Configurer correctement l'ExternalTaskSensor
    [ ] Stratégie incrémentale : calculer uniquement pour la date d'exécution
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

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

### TODO
Compléter les 4 tâches marquées NotImplementedError.
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

        TODO :
            1. Récupérer execution_date depuis context["data_interval_start"]
            2. Requête SQL :
               SELECT track_id,
                      COUNT(*) as total_streams,
                      COUNT(DISTINCT user_id) as unique_listeners,
                      SUM(duration_ms) as total_duration_ms,
                      ARRAY_AGG(DISTINCT geo_country) as countries
               FROM listening_events
               WHERE DATE(timestamp) = %(date)s AND completed = TRUE
               GROUP BY track_id
               ORDER BY total_streams DESC
               LIMIT 50
            3. Retourner la liste des agrégats
        """
        raise NotImplementedError("TODO : implémenter compute_top_tracks()")

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """
        Calcule les statistiques par artiste pour la date d'exécution.

        TODO :
            1. Jointure listening_events × tracks × artists
            2. GROUP BY artist_id, date
            3. Métriques : total_streams, unique_listeners, top_track_id
            4. Retourner la liste des stats artistes
        """
        raise NotImplementedError("TODO : implémenter compute_artist_stats()")

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """
        Calcule les métriques du réseau P2P pour la date d'exécution.

        TODO :
            1. Taux de cache_hit (event_source='cache' / total)
            2. Latence moyenne des transferts P2P
            3. Nombre de peers actifs uniques
            4. Distribution des écoutes par device_type et geo_country
            5. Retourner un dict de métriques
        """
        raise NotImplementedError("TODO : implémenter compute_p2p_metrics()")

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        """
        Écrit les agrégats dans PostgreSQL de façon idempotente.

        TODO :
            1. UPSERT dans daily_streams :
               INSERT INTO daily_streams (track_id, date, total_streams, ...)
               VALUES ... ON CONFLICT (track_id, date) DO UPDATE SET ...
            2. UPSERT dans artist_stats
            3. Logger les stats : "Top track: {title} avec {N} streams"
        """
        raise NotImplementedError("TODO : implémenter update_aggregates()")

    # ── Orchestration ─────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
