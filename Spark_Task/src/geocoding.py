"""
Geocoding module for OpenCage Geocoding API integration.
"""
import os
import time
from typing import Optional, Tuple
import requests


class OpenCageGeocoder:
    """OpenCage Geocoding API client for resolving coordinates from addresses."""
    
    BASE_URL = "https://api.opencagedata.com/geocode/v1/json"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the geocoder with an API key.
        
        Args:
            api_key: OpenCage API key. If not provided, reads from OPENCAGE_API_KEY env var.
        """
        self.api_key = api_key or os.environ.get("OPENCAGE_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OpenCage API key is required. Set OPENCAGE_API_KEY environment variable "
                "or pass api_key parameter."
            )
        self._request_count = 0
        self._last_request_time = 0
    
    def _rate_limit(self):
        """Apply rate limiting to respect API limits (1 request per second for free tier)."""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        if time_since_last < 1.0:
            time.sleep(1.0 - time_since_last)
        self._last_request_time = time.time()
    
    def geocode(self, city: str, country: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Geocode a city and country to get latitude and longitude.
        
        Args:
            city: City name
            country: Country code (e.g., 'US', 'GB', 'FR')
            
        Returns:
            Tuple of (latitude, longitude) or (None, None) if geocoding fails
        """
        self._rate_limit()
        
        query = f"{city}, {country}"
        
        params = {
            "q": query,
            "key": self.api_key,
            "limit": 1,
            "no_annotations": 1
        }
        
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("results") and len(data["results"]) > 0:
                geometry = data["results"][0].get("geometry", {})
                lat = geometry.get("lat")
                lng = geometry.get("lng")
                return (lat, lng)
            
            return (None, None)
            
        except requests.RequestException as e:
            print(f"Geocoding error for {query}: {e}")
            return (None, None)
        except (KeyError, ValueError) as e:
            print(f"Error parsing geocoding response for {query}: {e}")
            return (None, None)


def geocode_batch(records: list, api_key: Optional[str] = None) -> dict:
    """
    Geocode a batch of records with caching to minimize API calls.
    
    Args:
        records: List of dicts with 'city' and 'country' keys
        api_key: Optional OpenCage API key
        
    Returns:
        Dict mapping (city, country) tuples to (lat, lng) tuples
    """
    cache = {}
    geocoder = OpenCageGeocoder(api_key)
    
    unique_locations = set()
    for record in records:
        city = record.get("city")
        country = record.get("country")
        if city and country:
            unique_locations.add((city, country))
    
    for city, country in unique_locations:
        if (city, country) not in cache:
            lat, lng = geocoder.geocode(city, country)
            cache[(city, country)] = (lat, lng)
    
    return cache
