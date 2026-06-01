"""
SPOTIFY — Catalog Transformations
==================================
Fonctions de normalisation, validation et dédoublonnage du catalogue musical.
"""

def normalize_artist_name(name: str) -> str:
    """
    Normalise le nom d'un artiste :
    - Supprime les espaces en début et fin.
    - Convertit en Title Case (ex: 'the beatles' -> 'The Beatles').
    - Gère les cas None.
    - Préserve les caractères spéciaux (ex: 'björk' -> 'Björk').
    """
    if name is None:
        return None
    
    # Strip whitespace
    name = name.strip()
    
    # Title Case custom (preserve special chars casing)
    # capitalize() or title() can break M83 or characters in specific positions, so we capitalize words cleanly
    if not name:
        return name
        
    return " ".join(word.capitalize() for word in name.split())


def validate_track_schema(track: dict) -> list[str]:
    """
    Vérifie les champs obligatoires et la validité des types/valeurs pour une track.
    Retourne une liste d'erreurs (vide si valide).
    """
    errors = []
    
    # Champs obligatoires
    required_fields = ["id", "artist_id", "title", "duration_ms"]
    for field in required_fields:
        if field not in track or track[field] is None:
            errors.append(f"Missing required field: {field}")
            
    # Si les champs de base manquent, on s'arrête
    if errors:
        return errors
        
    # Validation du titre
    if not str(track["title"]).strip():
        errors.append("Empty track title")
        
    # Validation de la durée
    try:
        duration = int(track["duration_ms"])
        if duration <= 0:
            errors.append("Duration must be strictly positive")
        elif duration > 3600000:  # 1 heure max
            errors.append("Duration exceeds maximum limit of 1 hour (3600000 ms)")
    except (ValueError, TypeError):
        errors.append("Invalid duration format")
        
    return errors


def deduplicate_artists(artists: list[dict]) -> list[dict]:
    """
    Dédoublonne une liste d'artistes en se basant sur le couple (nom normalisé, label).
    Conserve le premier artiste unique rencontré.
    """
    seen = set()
    unique_artists = []
    
    for artist in artists:
        norm_name = normalize_artist_name(artist.get("name"))
        label = artist.get("label", "").strip()
        key = (norm_name, label)
        
        if key not in seen:
            seen.add(key)
            # update with normalized name
            artist_copy = artist.copy()
            artist_copy["name"] = norm_name
            unique_artists.append(artist_copy)
            
    return unique_artists


def deduplicate_tracks(tracks: list[dict]) -> list[dict]:
    """
    Dédoublonne une liste de tracks en se basant sur l'id de la track.
    """
    seen = set()
    unique_tracks = []
    
    for track in tracks:
        track_id = track.get("id")
        if track_id not in seen:
            seen.add(track_id)
            unique_tracks.append(track)
            
    return unique_tracks
