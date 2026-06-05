"""
DAG : recommendation_pipeline
================================
Generates personalized recommendations via collaborative filtering
and stores them in Redis + PostgreSQL.

Depends on aggregation_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (waits for aggregation_pipeline)
        → build_user_track_matrix()   ← user x track play matrix (7 days)
        → compute_recommendations()   ← cosine similarity collaborative filtering
        → store_recommendations()     ← Redis (TTL 24h) + PostgreSQL upsert
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

logger = logging.getLogger(__name__)

DAG_DOC = """
## recommendation_pipeline

### Role
Generates a top-10 recommendation list per active user
via collaborative filtering (cosine similarity between listening profiles).

### Dependencies
Waits for `aggregation_pipeline` to succeed via ExternalTaskSensor.

### Destinations
- Redis : key `reco:{user_id}` -> list of track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithm
Simplified collaborative filtering:
1. Build user x track matrix (listens from last 7 days)
2. Compute cosine similarity between users
3. For each user, recommend tracks liked by their closest neighbours
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400   # 24 hours
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7
MIN_LISTENS      = 3       # minimum distinct tracks to be an active user


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering -> recommendations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        execution_delta=timedelta(hours=1),
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """
        Builds the user x track play count matrix for the last 7 days.
        Only keeps users with >= MIN_LISTENS distinct tracks (active users).
        Returns: {
            "matrix": {user_id: {track_id: play_count}},
            "all_tracks": [track_id, ...]
        }
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                user_id::text,
                track_id::text,
                COUNT(*) AS play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days'
              AND completed = TRUE
            GROUP BY user_id, track_id
            """
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # Build {user_id: {track_id: play_count}}
        raw_matrix = {}
        for user_id, track_id, play_count in rows:
            raw_matrix.setdefault(user_id, {})[track_id] = play_count

        # Keep only active users (>= MIN_LISTENS distinct tracks)
        matrix = {
            uid: tracks
            for uid, tracks in raw_matrix.items()
            if len(tracks) >= MIN_LISTENS
        }

        all_tracks = list({t for tracks in matrix.values() for t in tracks})

        logger.info(
            "build_user_track_matrix: %d active users, %d unique tracks",
            len(matrix), len(all_tracks),
        )
        return {"matrix": matrix, "all_tracks": all_tracks}

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """
        Computes recommendations using cosine similarity between user profiles.
        For each user: find TOP_N neighbours, recommend tracks they liked
        that the current user has not listened to.
        Returns: {user_id: [(track_id, score), ...]}
        """
        import numpy as np

        matrix     = matrix_data["matrix"]
        all_tracks = matrix_data["all_tracks"]

        if not matrix or not all_tracks:
            logger.warning("Empty matrix -- no recommendations to compute")
            return {}

        user_ids    = list(matrix.keys())
        track_index = {t: i for i, t in enumerate(all_tracks)}
        n_users     = len(user_ids)
        n_tracks    = len(all_tracks)

        # Build dense matrix (n_users x n_tracks)
        M = np.zeros((n_users, n_tracks), dtype=np.float32)
        for u_idx, uid in enumerate(user_ids):
            for track_id, play_count in matrix[uid].items():
                t_idx = track_index[track_id]
                M[u_idx, t_idx] = float(play_count)

        # Cosine similarity: normalise rows then dot product
        norms = np.linalg.norm(M, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        M_norm     = M / norms
        sim_matrix = M_norm @ M_norm.T   # (n_users x n_users)

        recommendations = {}

        for u_idx, uid in enumerate(user_ids):
            user_sim       = sim_matrix[u_idx].copy()
            user_sim[u_idx] = -1.0   # exclude self

            # TOP_N most similar neighbours
            top_n             = min(TOP_N_RECO, n_users - 1)
            neighbour_indices = np.argsort(user_sim)[::-1][:top_n]

            # Tracks already listened to by this user
            listened = set(matrix[uid].keys())

            # Score = sum(sim * play_count) for each unseen track
            scores = {}
            for n_idx in neighbour_indices:
                sim_score = float(user_sim[n_idx])
                if sim_score <= 0:
                    continue
                neighbour_uid = user_ids[n_idx]
                for track_id, play_count in matrix[neighbour_uid].items():
                    if track_id not in listened:
                        scores[track_id] = scores.get(track_id, 0.0) + sim_score * play_count

            # Top TOP_N_RECO tracks by score
            top_tracks = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:TOP_N_RECO]
            if top_tracks:
                recommendations[uid] = top_tracks

        logger.info(
            "compute_recommendations: %d/%d users have recommendations",
            len(recommendations), n_users,
        )
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stores recommendations in Redis (TTL 24h) and PostgreSQL (upsert).
        Redis key: reco:{user_id}  ->  JSON list of track_ids
        PostgreSQL: ON CONFLICT (user_id, track_id) DO UPDATE
        """
        import redis as redis_lib

        if not recommendations:
            logger.warning("No recommendations to store")
            return {"users_with_recos": 0, "total_recommendations": 0}

        # -- Redis --------------------------------------------------------
        r = redis_lib.from_url(REDIS_URL, decode_responses=True)
        r_db0 = redis_lib.from_url("redis://redis:6379/0", decode_responses=True)
        for user_id, track_scores in recommendations.items():
            track_ids = [t for t, _ in track_scores]
            r.setex(f"reco:{user_id}", RECO_TTL_SECONDS, json.dumps(track_ids))
            r_db0.setex(f"reco:{user_id}", RECO_TTL_SECONDS, json.dumps(track_ids))

        # -- PostgreSQL ---------------------------------------------------
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cur  = conn.cursor()

        total = 0
        try:
            for user_id, track_scores in recommendations.items():
                for rank, (track_id, score) in enumerate(track_scores, start=1):
                    cur.execute(
                        """
                        INSERT INTO recommendations
                            (user_id, track_id, score, rank, generated_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON CONFLICT (user_id, track_id) DO UPDATE SET
                            score        = EXCLUDED.score,
                            rank         = EXCLUDED.rank,
                            generated_at = NOW()
                        """,
                        (user_id, track_id, round(score, 6), rank),
                    )
                    total += 1
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error("store_recommendations failed: %s", e)
            raise
        finally:
            cur.close()
            conn.close()

        stats = {
            "users_with_recos":      len(recommendations),
            "total_recommendations": total,
        }
        logger.info("store_recommendations done: %s", stats)
        return stats

    # -- Orchestration ----------------------------------------------------
    matrix          = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)
