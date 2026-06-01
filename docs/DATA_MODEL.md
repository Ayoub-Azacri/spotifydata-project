# Database Schema & Entity Relationship Diagram (ERD)

This document describes the relational database design for the SPOTIFY distributed data platform.

---

## 📊 Entity Relationship Diagram (ERD)

```mermaid
erDiagram
    GENRES ||--o{ TRACKS : "categorises"
    ARTISTS ||--o{ ALBUMS : "creates"
    ARTISTS ||--o{ TRACKS : "performs"
    ALBUMS ||--o{ TRACKS : "contains"
    TRACKS ||--o{ LISTENING-EVENTS : "listened_in"
    PEERS ||--o{ LISTENING-EVENTS : "streams_from"
    TRACKS ||--o{ DAILY-STREAMS : "aggregates"
    TRACKS ||--o{ RECOMMENDATIONS : "recommended"
    
    ARTISTS {
        uuid id PK
        varchar name
        varchar country
        varchar label
        text_array genres
        int monthly_listeners
        timestamp created_at
        timestamp updated_at
    }

    ALBUMS {
        uuid id PK
        uuid artist_id FK
        varchar title
        int release_year
        int total_tracks
        timestamp created_at
    }

    TRACKS {
        uuid id PK
        uuid album_id FK
        uuid artist_id FK
        varchar title
        int duration_ms
        varchar genre
        int bpm
        boolean explicit
        varchar audio_file_path
        timestamp created_at
        timestamp updated_at
    }

    PEERS {
        uuid id PK
        varchar peer_name
        varchar ip_address
        varchar device_type
        varchar geo_country
        varchar geo_city
        varchar status
        text_array cached_tracks
        timestamp last_seen
        timestamp created_at
    }

    LISTENING-EVENTS {
        uuid id PK
        uuid user_id
        uuid track_id FK
        uuid source_peer_id FK
        timestamp timestamp
        int duration_ms
        varchar device_type
        varchar geo_country
        boolean completed
        varchar event_source
        timestamp created_at
    }

    DAILY-STREAMS {
        uuid track_id PK, FK
        date date PK
        bigint total_streams
        bigint unique_listeners
        bigint total_duration_ms
        text_array countries
        timestamp updated_at
    }

    RECOMMENDATIONS {
        uuid user_id PK
        uuid track_id PK, FK
        float score
        timestamp generated_at
    }
    
    DEAD-LETTER-EVENTS {
        uuid id PK
        varchar original_topic
        jsonb payload
        varchar error_type
        text error_message
        int retry_count
        varchar status
        timestamp created_at
        timestamp last_retry_at
        timestamp resolved_at
    }
```

---

## 🗃️ Table Specifications & Types

### 1. `artists`
Stores music creator profiles.
- **Constraints**: Composite unique key on `(name, label)` to prevent duplicated catalog entries.
- **Data types**: `genres` is stored as a Postgres Text Array (`TEXT[]`) for flexible indexing.

### 2. `albums`
Aggregates tracks under a release structure.
- **Foreign Key**: `artist_id` references `artists(id)` with ON DELETE CASCADE.

### 3. `tracks`
Core music catalog items.
- **Foreign Key**: `album_id` (nullable for singles) and `artist_id` (mandatory).

### 4. `peers`
Simulated P2P network nodes.
- **Tracking**: `last_seen` timestamp to monitor active nodes.

### 5. `listening_events`
Transactional streams of play logs.
- **Partitioning Strategy**: Indexed on `date_trunc('hour', timestamp)` to enable efficient hourly batch execution.

### 6. `daily_streams`
Pre-aggregated catalog metrics for batch reporting.
- **Composite Primary Key**: `(track_id, date)` ensures single-day historical tracking.

### 7. `dead_letter_events` (DLQ)
Quarantines invalid, corrupted, or schema-mismatched events.
- **Data type**: `payload` uses binary JSON (`JSONB`) to store any malformed structure for analysis and reprocessing.
