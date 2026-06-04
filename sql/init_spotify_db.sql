-- ============================================================
-- SPOTIFY — Initialisation de la base de données
-- Script exécuté automatiquement au démarrage de PostgreSQL
-- ============================================================

-- Créer la base et l'utilisateur SPOTIFY
CREATE USER spotify WITH PASSWORD 'spotify';
CREATE DATABASE spotify OWNER spotify;
GRANT ALL PRIVILEGES ON DATABASE spotify TO spotify;

\connect spotify

-- ============================================================
-- MODULE 1 — CATALOGUE MUSICAL
-- ============================================================

CREATE TABLE genres (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE artists (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        VARCHAR(255) NOT NULL,
    country     VARCHAR(100),
    label       VARCHAR(255),
    genres      TEXT[],                    -- array de genre_names
    monthly_listeners INT DEFAULT 0,
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE(name, label)
);

CREATE TABLE albums (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    artist_id   UUID NOT NULL REFERENCES artists(id),
    title       VARCHAR(255) NOT NULL,
    release_year INT,
    total_tracks INT,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE tracks (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    album_id        UUID REFERENCES albums(id),
    artist_id       UUID NOT NULL REFERENCES artists(id),
    title           VARCHAR(255) NOT NULL,
    duration_ms     INT NOT NULL,
    genre           VARCHAR(100),
    bpm             INT,
    explicit        BOOLEAN DEFAULT FALSE,
    audio_file_path VARCHAR(500),          -- chemin MinIO simulé
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- MODULE 1 — RÉSEAU P2P ET UTILISATEURS
-- ============================================================

CREATE TABLE peers (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    peer_name       VARCHAR(100) NOT NULL,
    ip_address      VARCHAR(45),
    device_type     VARCHAR(50),           -- mobile, desktop, speaker
    geo_country     VARCHAR(100),
    geo_city        VARCHAR(100),
    status          VARCHAR(20) DEFAULT 'offline', -- online, offline, streaming
    cached_tracks   TEXT[],                -- track_ids en cache local
    last_seen       TIMESTAMP DEFAULT NOW(),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- MODULE 1 — ÉVÉNEMENTS D'ÉCOUTE
-- ============================================================

CREATE TABLE listening_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL,
    track_id        UUID NOT NULL REFERENCES tracks(id),
    source_peer_id  UUID REFERENCES peers(id),
    timestamp       TIMESTAMP NOT NULL,
    duration_ms     INT,                   -- durée réellement écoutée
    device_type     VARCHAR(50),
    geo_country     VARCHAR(100),
    completed       BOOLEAN DEFAULT FALSE, -- écoute complète (>30s)
    event_source    VARCHAR(20) DEFAULT 'p2p', -- p2p, direct, cache
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_listening_events_user_id ON listening_events(user_id);
CREATE INDEX idx_listening_events_track_id ON listening_events(track_id);
CREATE INDEX idx_listening_events_timestamp ON listening_events(timestamp);
CREATE INDEX idx_listening_events_ts_partition ON listening_events(date_trunc('hour', timestamp));

-- ============================================================
-- MODULE 1 — AGRÉGATS
-- ============================================================

CREATE TABLE daily_streams (
    track_id        UUID NOT NULL REFERENCES tracks(id),
    date            DATE NOT NULL,
    total_streams   BIGINT DEFAULT 0,
    unique_listeners BIGINT DEFAULT 0,
    total_duration_ms BIGINT DEFAULT 0,
    countries       TEXT[],
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (track_id, date)
);

CREATE TABLE artist_stats (
    artist_id       UUID NOT NULL REFERENCES artists(id),
    date            DATE NOT NULL,
    total_streams   BIGINT DEFAULT 0,
    unique_listeners BIGINT DEFAULT 0,
    top_track_id    UUID,
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (artist_id, date)
);

CREATE TABLE recommendations (
    user_id         UUID NOT NULL,
    track_id        UUID NOT NULL REFERENCES tracks(id),
    score           FLOAT NOT NULL,
    rank            INT,
    generated_at    TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, track_id)
);

-- ============================================================
-- MODULE 1 — DEAD LETTER QUEUE
-- ============================================================

CREATE TABLE dead_letter_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_topic  VARCHAR(100),           -- source (redis_pub_sub, kafka_topic...)
    payload         JSONB NOT NULL,
    error_type      VARCHAR(100),
    error_message   TEXT,
    retry_count     INT DEFAULT 0,
    status          VARCHAR(20) DEFAULT 'pending', -- pending, reprocessed, abandoned
    created_at      TIMESTAMP DEFAULT NOW(),
    last_retry_at   TIMESTAMP,
    resolved_at     TIMESTAMP
);

CREATE INDEX idx_dlq_status ON dead_letter_events(status);
CREATE INDEX idx_dlq_created_at ON dead_letter_events(created_at);

-- ============================================================
-- MODULE 2 — TEMPS RÉEL (tables alimentées par Spark)
-- ============================================================

CREATE TABLE realtime_top_tracks (
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    track_id        UUID NOT NULL REFERENCES tracks(id),
    stream_count    BIGINT DEFAULT 0,
    unique_listeners BIGINT DEFAULT 0,
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (window_start, track_id)
);

CREATE TABLE fraud_detections (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID,
    peer_id         UUID,
    fraud_type      VARCHAR(100),          -- bot_stream, free_rider, burst_listen
    suspicion_score FLOAT,
    evidence        JSONB,
    window_start    TIMESTAMP,
    window_end      TIMESTAMP,
    detected_at     TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- MODULE 3 — INTER-GROUPES
-- ============================================================

CREATE TABLE federated_catalog (
    track_id        UUID NOT NULL,
    source_group    VARCHAR(50) NOT NULL,   -- groupe d'origine (groupe-a, groupe-b...)
    artist_name     VARCHAR(255),
    track_title     VARCHAR(255),
    duration_ms     INT,
    genre           VARCHAR(100),
    audio_peer_endpoint VARCHAR(500),       -- endpoint pour télécharger
    ingested_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (track_id, source_group)
);

-- ============================================================
-- DONNÉES DE RÉFÉRENCE
-- ============================================================

INSERT INTO genres (name) VALUES
    ('Pop'), ('Rock'), ('Hip-Hop'), ('Electronic'), ('Jazz'),
    ('Classical'), ('R&B'), ('Metal'), ('Folk'), ('Latin');

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO spotify;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO spotify;

COMMENT ON TABLE listening_events IS 'Événements d''écoute générés par le simulateur P2P. Partitionnable par timestamp pour la parallélisation.';
COMMENT ON TABLE dead_letter_events IS 'Dead Letter Queue — événements défectueux isolés pour audit et retraitement.';
COMMENT ON TABLE realtime_top_tracks IS 'Alimentée par Spark Structured Streaming (job streaming_trends_job). Fenêtres de 5 min.';
