"""
SPOTIFY — Events Transformations
==================================
Fonctions de validation et d'enrichissement des événements d'écoute.
"""

import json
from datetime import datetime, timezone

def is_valid_listening_event(event: dict) -> bool:
    """
    Valide un événement d'écoute.
    Champs obligatoires : event_id, user_id, track_id, timestamp, duration_ms
    """
    required_fields = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]
    
    # 1. Vérifier les champs obligatoires
    for field in required_fields:
        if field not in event or event[field] is None:
            return False
            
    # 2. Valider le format du timestamp
    try:
        # Remplacer Z par +00:00 pour fromisoformat
        ts_str = event["timestamp"].replace("Z", "+00:00")
        ts = datetime.fromisoformat(ts_str)
        
        # S'assurer que ts est aware pour la comparaison
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
            
        now = datetime.now(timezone.utc)
        # Un timestamp dans le futur est suspect (seuil de 5 min)
        if ts > now + timedelta(minutes=5):
            return False
    except (ValueError, TypeError, NameError):
        # timedelta might not be imported if I'm not careful
        return False
        
    # 3. Valider la durée
    try:
        duration = int(event["duration_ms"])
        if duration <= 0:
            return False
    except (ValueError, TypeError):
        return False
        
    return True

from datetime import timedelta

def is_valid_p2p_event(event: dict) -> bool:
    """
    Valide un événement réseau P2P.
    """
    required_fields = ["event_id", "event_type", "peer_id", "timestamp"]
    for field in required_fields:
        if field not in event or event[field] is None:
            return False
    return True
