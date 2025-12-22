"""
Geohash utility module for generating geohashes from coordinates.
"""
import geohash as gh
from typing import Optional


def generate_geohash(lat: Optional[float], lng: Optional[float], precision: int = 4) -> Optional[str]:
    """
    Generate a geohash from latitude and longitude coordinates.
    
    Args:
        lat: Latitude coordinate
        lng: Longitude coordinate  
        precision: Geohash precision (default 4 characters)
        
    Returns:
        Geohash string of specified precision, or None if coordinates are invalid
    """
    if lat is None or lng is None:
        return None
    
    try:
        # Validate coordinate ranges
        if not (-90 <= lat <= 90) or not (-180 <= lng <= 180):
            return None
        
        # Clamp to open interval to avoid boundary errors in geohash library
        eps = 1e-9
        safe_lat = min(max(lat, -90 + eps), 90 - eps)
        safe_lng = min(max(lng, -180 + eps), 180 - eps)
        
        # Generate geohash with specified precision
        return gh.encode(safe_lat, safe_lng, precision=precision)
    except (ValueError, TypeError):
        return None


def decode_geohash(geohash_str: str) -> tuple:
    """
    Decode a geohash string back to latitude and longitude.
    
    Args:
        geohash_str: Geohash string
        
    Returns:
        Tuple of (latitude, longitude)
    """
    if not geohash_str:
        return (None, None)
    try:
        return gh.decode(geohash_str)
    except (ValueError, TypeError):
        return (None, None)


def get_geohash_neighbors(geohash_str: str) -> dict:
    """
    Get all neighboring geohashes for a given geohash.
    
    Args:
        geohash_str: Geohash string
        
    Returns:
        Dict with direction keys and geohash values
    """
    if not geohash_str:
        return {}
    try:
        neighbors = gh.neighbors(geohash_str)
        # geohash library returns a list; convert to a direction-indexed dict for clarity
        if isinstance(neighbors, list):
            directions = ["n", "ne", "e", "se", "s", "sw", "w", "nw"]
            return {directions[i]: neighbors[i] for i in range(min(len(neighbors), len(directions)))}
        return neighbors
    except (ValueError, TypeError):
        return {}
