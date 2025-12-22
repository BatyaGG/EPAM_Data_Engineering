"""
Tests for geocoding functionality.
"""
import pytest
import os
import sys
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.geocoding import OpenCageGeocoder, geocode_batch


class TestOpenCageGeocoder:
    """Tests for OpenCageGeocoder class."""
    
    def test_init_with_api_key(self):
        """Test initialization with API key."""
        geocoder = OpenCageGeocoder(api_key="test_key")
        assert geocoder.api_key == "test_key"
    
    def test_init_without_api_key_raises_error(self):
        """Test initialization without API key raises ValueError."""
        # Clear environment variable if set
        original = os.environ.pop("OPENCAGE_API_KEY", None)
        
        try:
            with pytest.raises(ValueError):
                OpenCageGeocoder()
        finally:
            if original:
                os.environ["OPENCAGE_API_KEY"] = original
    
    def test_init_from_environment_variable(self):
        """Test initialization from environment variable."""
        original = os.environ.get("OPENCAGE_API_KEY")
        os.environ["OPENCAGE_API_KEY"] = "env_test_key"
        
        try:
            geocoder = OpenCageGeocoder()
            assert geocoder.api_key == "env_test_key"
        finally:
            if original:
                os.environ["OPENCAGE_API_KEY"] = original
            else:
                os.environ.pop("OPENCAGE_API_KEY", None)
    
    @patch("src.geocoding.requests.get")
    def test_geocode_success(self, mock_get):
        """Test successful geocoding."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "results": [
                {
                    "geometry": {
                        "lat": 40.7128,
                        "lng": -74.0060
                    }
                }
            ]
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        geocoder = OpenCageGeocoder(api_key="test_key")
        lat, lng = geocoder.geocode("New York", "US")
        
        assert lat == 40.7128
        assert lng == -74.0060
    
    @patch("src.geocoding.requests.get")
    def test_geocode_no_results(self, mock_get):
        """Test geocoding with no results."""
        mock_response = Mock()
        mock_response.json.return_value = {"results": []}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        geocoder = OpenCageGeocoder(api_key="test_key")
        lat, lng = geocoder.geocode("Unknown City", "XX")
        
        assert lat is None
        assert lng is None
    
    @patch("src.geocoding.requests.get")
    def test_geocode_request_error(self, mock_get):
        """Test geocoding with request error."""
        import requests
        mock_get.side_effect = requests.RequestException("Connection error")
        
        geocoder = OpenCageGeocoder(api_key="test_key")
        lat, lng = geocoder.geocode("New York", "US")
        
        assert lat is None
        assert lng is None


class TestGeocodeBatch:
    """Tests for geocode_batch function."""
    
    @patch("src.geocoding.OpenCageGeocoder")
    def test_batch_geocode_caches_results(self, mock_geocoder_class):
        """Test that batch geocoding caches results for duplicate locations."""
        mock_geocoder = Mock()
        mock_geocoder.geocode.return_value = (40.7128, -74.0060)
        mock_geocoder_class.return_value = mock_geocoder
        
        records = [
            {"city": "New York", "country": "US"},
            {"city": "New York", "country": "US"},  # Duplicate
            {"city": "Paris", "country": "FR"},
        ]
        
        result = geocode_batch(records, api_key="test_key")
        
        # Should only call geocode twice (for unique locations)
        assert mock_geocoder.geocode.call_count == 2
        assert ("New York", "US") in result
        assert ("Paris", "FR") in result
    
    @patch("src.geocoding.OpenCageGeocoder")
    def test_batch_geocode_handles_missing_fields(self, mock_geocoder_class):
        """Test that batch geocoding handles records with missing fields."""
        mock_geocoder = Mock()
        mock_geocoder.geocode.return_value = (40.7128, -74.0060)
        mock_geocoder_class.return_value = mock_geocoder
        
        records = [
            {"city": "New York", "country": "US"},
            {"city": None, "country": "US"},  # Missing city
            {"city": "Paris"},  # Missing country
        ]
        
        result = geocode_batch(records, api_key="test_key")
        
        # Should only process the valid record
        assert mock_geocoder.geocode.call_count == 1
        assert ("New York", "US") in result


class TestGeocodingIntegration:
    """Integration tests for geocoding (requires API key)."""
    
    @pytest.mark.skipif(
        not os.environ.get("OPENCAGE_API_KEY"),
        reason="OPENCAGE_API_KEY not set"
    )
    def test_real_geocoding_new_york(self):
        """Test real geocoding for New York (integration test)."""
        geocoder = OpenCageGeocoder()
        lat, lng = geocoder.geocode("New York", "US")
        
        assert lat is not None
        assert lng is not None
        # NYC should be approximately at these coordinates
        assert 40 < lat < 41
        assert -75 < lng < -73
    
    @pytest.mark.skipif(
        not os.environ.get("OPENCAGE_API_KEY"),
        reason="OPENCAGE_API_KEY not set"
    )
    def test_real_geocoding_paris(self):
        """Test real geocoding for Paris (integration test)."""
        geocoder = OpenCageGeocoder()
        lat, lng = geocoder.geocode("Paris", "FR")
        
        assert lat is not None
        assert lng is not None
        # Paris should be approximately at these coordinates
        assert 48 < lat < 49
        assert 2 < lng < 3
