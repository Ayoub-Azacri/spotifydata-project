"""
SPOTIFY — Simulateur P2P
========================
Ce simulateur génère des événements réalistes d'un réseau peer-to-peer
de streaming musical. Il publie dans Redis pub/sub (Phase 1) et dans
Kafka (Phase 2, après décommentage).

Usage :
    python -m src.p2p_simulator.simulator --peers 10 --rate 5
    python -m src.p2p_simulator.simulator --mode fraud --peers 5
    python -m src.p2p_simulator.simulator --mode late_events

TODO Phase 1 :  Compléter _generate_listening_event() et _publish_to_redis()
TODO Phase 2 :  Activer _publish_to_kafka() et le mode fraude
"""

import argparse
import json
import logging
import random
import signal
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import redis

# Phase 2 — décommenter quand Kafka est prêt
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("p2p_simulator")


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

import os
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/1")

# Tente de résoudre kafka-1 pour déterminer si on est dans Docker ou en local
import socket
try:
    socket.gethostbyname("kafka-1")
    KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-1:9092")
except socket.gaierror:
    KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")

TOPICS = {
    "listening":   "listening_events",
    "p2p_network": "p2p_network_events",
}

DEVICE_TYPES = ["mobile", "desktop", "smart_speaker", "web", "tv"]
GEO_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP", "KR", "AU"]
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]  # pondéré : 60% P2P


# ─────────────────────────────────────────────────────────────
# DONNÉES SIMULÉES
# ─────────────────────────────────────────────────────────────

# Ces UUIDs seront remplacés par les vrais IDs depuis PostgreSQL
# Une fois votre base peuplée, charger dynamiquement avec _load_catalog()
SAMPLE_TRACKS = [
    {"id": str(uuid.uuid4()), "title": f"Track {i}", "duration_ms": random.randint(120000, 300000)}
    for i in range(50)
]

SAMPLE_USERS = [str(uuid.uuid4()) for _ in range(200)]
SAMPLE_PEERS = [str(uuid.uuid4()) for _ in range(20)]


# ─────────────────────────────────────────────────────────────
# SIMULATEUR PRINCIPAL
# ─────────────────────────────────────────────────────────────

class P2PSimulator:
    """
    Simulateur du réseau P2P SPOTIFY.

    Génère deux types d'événements :
    - listening_events   : un utilisateur écoute un morceau via un peer
    - p2p_network_events : connexion/déconnexion/transfert entre peers
    """

    def __init__(
        self,
        n_peers: int = 10,
        events_per_second: float = 5.0,
        mode: str = "normal",
    ):
        self.n_peers = n_peers
        self.events_per_second = events_per_second
        self.mode = mode
        self.running = True
        self.event_count = 0

        # Connexion Redis
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)

        # Phase 2 — Kafka producer
        self.kafka_producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "enable.idempotence": True
        })

        # Peers actifs simulés
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        # Charger le catalogue et enregistrer les peers pour satisfaire les clés étrangères Postgres
        self._load_catalog_and_register_peers()

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        logger.info(f"Simulateur démarré | mode={mode} | peers={n_peers} | rate={events_per_second} evt/s")

    def _load_catalog_and_register_peers(self):
        """Charge les track_ids depuis PostgreSQL et insère les peers actifs."""
        import psycopg2
        global SAMPLE_TRACKS
        try:
            # Tente de se connecter en local d'abord, puis via le réseau docker
            try:
                conn = psycopg2.connect("postgresql://spotify:spotify@localhost:5432/spotify")
            except Exception:
                conn = psycopg2.connect("postgresql://spotify:spotify@postgres:5432/spotify")
                
            cur = conn.cursor()
            
            # 1. Charger les tracks existantes
            cur.execute("SELECT id, duration_ms, title FROM tracks LIMIT 500")
            rows = cur.fetchall()
            if rows:
                SAMPLE_TRACKS = [{"id": str(r[0]), "title": r[2], "duration_ms": r[1]} for r in rows]
                logger.info(f"Chargé {len(SAMPLE_TRACKS)} tracks depuis PostgreSQL.")
            else:
                logger.warning("Aucune track trouvée dans PostgreSQL. Utilisation de SAMPLE_TRACKS par défaut.")
                
            # 2. Enregistrer les peers actifs
            for peer_id in self.active_peers:
                cur.execute(
                    """
                    INSERT INTO peers (id, peer_name, status)
                    VALUES (%s, %s, 'online')
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (peer_id, f"Peer-{peer_id[:8]}",)
                )
            conn.commit()
            logger.info(f"Enregistré {len(self.active_peers)} peers actifs dans PostgreSQL.")
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Échec de connexion PostgreSQL / chargement catalogue : {e}")

    def run(self):
        """Boucle principale : génère et publie des événements en continu."""
        interval = 1.0 / self.events_per_second

        while self.running:
            try:
                # Alterner listening et réseau P2P (80% / 20%)
                if random.random() < 0.8:
                    event = self._generate_listening_event()
                    self._publish_event("listening", event)
                else:
                    event = self._generate_p2p_network_event()
                    self._publish_event("p2p_network", event)

                self.event_count += 1

                if self.event_count % 100 == 0:
                    logger.info(f"Événements publiés : {self.event_count}")

                time.sleep(interval)

            except Exception as e:
                logger.error(f"Erreur lors de la génération d'événement : {e}")
                time.sleep(1)

    # ── Génération d'événements ──────────────────────────────

    def _generate_listening_event(self) -> dict:
        """
        Génère un événement d'écoute.
        """
        track = random.choice(SAMPLE_TRACKS)
        duration_ms = random.randint(30000, track["duration_ms"])
        
        event = {
            "event_id":    str(uuid.uuid4()),
            "user_id":     random.choice(SAMPLE_USERS),
            "track_id":    track["id"],
            "source_peer": random.choice(self.active_peers),
            "timestamp":   datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "duration_ms": duration_ms,
            "device_type": random.choice(DEVICE_TYPES),
            "geo_country": random.choice(GEO_COUNTRIES),
            "completed":   duration_ms > 30000,
            "event_source": random.choice(EVENT_SOURCES)
        }

        # Mode fraud (Phase 2)
        if self.mode == "fraud" and random.random() < 0.3:
            event["duration_ms"] = random.randint(100, 4999)
            event["completed"] = False

        # Mode late_events (Phase 2)
        if self.mode == "late_events" and random.random() < 0.4:
            delay_minutes = random.randint(5, 30)
            ts = datetime.now(timezone.utc) - timedelta(minutes=delay_minutes)
            event["timestamp"] = ts.isoformat() + "Z"

        return event

    def _generate_p2p_network_event(self) -> dict:
        """
        Génère un événement réseau P2P.
        """
        event_type = random.choice([
            "peer_connect", "peer_disconnect",
            "chunk_transfer", "cache_hit", "cache_miss"
        ])

        event = {
            "event_id":   str(uuid.uuid4()),
            "event_type": event_type,
            "peer_id":    random.choice(self.active_peers),
            "timestamp":  datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }

        if event_type == "chunk_transfer":
            event["target_peer"] = random.choice(self.active_peers)
            event["chunk_id"] = str(uuid.uuid4())
            event["size_bytes"] = random.randint(1024, 1024 * 1024)
        
        if event_type in ["cache_hit", "cache_miss"]:
            event["track_id"] = random.choice(SAMPLE_TRACKS)["id"]

        return event

    # ── Publication ──────────────────────────────────────────

    def _publish_event(self, topic_key: str, event: dict):
        """Publie un événement dans Redis et (Phase 2) dans Kafka."""
        payload = json.dumps(event)
        channel = TOPICS[topic_key]

        self._publish_to_redis(channel, payload)
        # Phase 2 — décommenter quand Kafka est prêt
        self._publish_to_kafka(channel, event.get("user_id", event.get("peer_id", "")), payload)

    def _publish_to_redis(self, channel: str, payload: str):
        """
        Publie payload dans le channel Redis via pub/sub et lpush dans une LIST.
        """
        try:
            # 1. Pub/Sub (pour Spark Phase 2)
            self.redis.publish(channel, payload)
            
            # 2. LIST (pour Airflow Phase 1)
            list_key = f"list:{channel}"
            self.redis.lpush(list_key, payload)
            # Limiter la taille de la liste pour éviter l'explosion mémoire (ex: 10000 derniers events)
            self.redis.ltrim(list_key, 0, 9999)
            
        except Exception as e:
            logger.error(f"Échec de publication Redis sur {channel} : {e}")

    def _publish_to_kafka(self, topic: str, key: str, payload: str):
        """
        Publier payload dans le topic Kafka.
        """
        try:
            self.kafka_producer.produce(topic, key=key, value=payload)
            self.kafka_producer.poll(0)
        except Exception as e:
            logger.error(f"Échec de publication Kafka sur {topic} : {e}")

    def _shutdown(self, signum, frame):
        logger.info(f"Arrêt du simulateur (signal {signum}) — {self.event_count} événements publiés")
        self.running = False
        try:
            logger.info("Flushing Kafka producer...")
            self.kafka_producer.flush(timeout=5)
        except Exception as e:
            logger.error(f"Erreur lors du flush du Kafka producer : {e}")


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SPOTIFY P2P Simulator")
    parser.add_argument("--peers",  type=int,   default=10,     help="Nombre de peers simulés")
    parser.add_argument("--rate",   type=float, default=5.0,    help="Événements par seconde")
    parser.add_argument("--mode",   type=str,   default="normal",
                        choices=["normal", "fraud", "late_events", "chaos"],
                        help="Mode de simulation")
    args = parser.parse_args()

    simulator = P2PSimulator(
        n_peers=args.peers,
        events_per_second=args.rate,
        mode=args.mode,
    )
    simulator.run()


if __name__ == "__main__":
    main()
