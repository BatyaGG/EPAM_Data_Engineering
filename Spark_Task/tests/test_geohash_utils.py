"""
Tests for geohash utility functions.
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.geohash_utils import generate_geohash, decode_geohash, get_geohash_neighbors


class TestGenerateGeohash:
    """Tests for generate_geohash function."""
    
    def test_valid_coordinates_returns_4_char_geohash(self):
        """Test that valid coordinates return a 4-character geohash."""
        result = generate_geohash(40.7128, -74.0060, precision=4)
        
        assert result is not None
        assert len(result) == 4
        assert result == "dr5r"  # Known geohash for NYC
    
    def test_paris_coordinates(self):
        """Test geohash for Paris coordinates."""
        result = generate_geohash(48.8566, 2.3522, precision=4)
        
        assert result is not None
        assert len(result) == 4
        assert result.startswith("u09")
    
    def test_london_coordinates(self):
        """Test geohash for London coordinates."""
        result = generate_geohash(51.5074, -0.1278, precision=4)
        
        assert result is not None
        assert len(result) == 4
        assert result.startswith("gcpv")
    
    def test_null_latitude_returns_none(self):
        """Test that null latitude returns None."""
        result = generate_geohash(None, -74.0060)
        assert result is None
    
    def test_null_longitude_returns_none(self):
        """Test that null longitude returns None."""
        result = generate_geohash(40.7128, None)
        assert result is None
    
    def test_both_null_returns_none(self):
        """Test that both null coordinates return None."""
        result = generate_geohash(None, None)
        assert result is None
    
    def test_invalid_latitude_too_high_returns_none(self):
        """Test that latitude > 90 returns None."""
        result = generate_geohash(91.0, 0.0)
        assert result is None
    
    def test_invalid_latitude_too_low_returns_none(self):
        """Test that latitude < -90 returns None."""
        result = generate_geohash(-91.0, 0.0)
        assert result is None
    
    def test_invalid_longitude_too_high_returns_none(self):
        """Test that longitude > 180 returns None."""
        result = generate_geohash(0.0, 181.0)
        assert result is None
    
    def test_invalid_longitude_too_low_returns_none(self):
        """Test that longitude < -180 returns None."""
        result = generate_geohash(0.0, -181.0)
        assert result is None
    
    def test_different_precision(self):
        """Test that precision parameter works correctly."""
        result_4 = generate_geohash(40.7128, -74.0060, precision=4)
        result_6 = generate_geohash(40.7128, -74.0060, precision=6)
        result_8 = generate_geohash(40.7128, -74.0060, precision=8)
        
        assert len(result_4) == 4
        assert len(result_6) == 6
        assert len(result_8) == 8
        assert result_6.startswith(result_4)
        assert result_8.startswith(result_6)
    
    def test_boundary_coordinates(self):
        """Test boundary coordinate values."""
        # North Pole
        result = generate_geohash(90.0, 0.0, precision=4)
        assert result is not None
        
        # South Pole
        result = generate_geohash(-90.0, 0.0, precision=4)
        assert result is not None
        
        # Date Line
        result = generate_geohash(0.0, 180.0, precision=4)
        assert result is not None
        
        result = generate_geohash(0.0, -180.0, precision=4)
        assert result is not None


class TestDecodeGeohash:
    """Tests for decode_geohash function."""
    
    def test_decode_valid_geohash(self):
        """Test decoding a valid geohash."""
        lat, lng = decode_geohash("dr5r")
        
        assert lat is not None
        assert lng is not None
        # NYC area
        assert 40 < lat < 41
        assert -75 < lng < -73
    
    def test_decode_invalid_geohash(self):
        """Test decoding an invalid geohash returns None tuple."""
        lat, lng = decode_geohash("")
        assert lat is None or lng is None


class TestGetGeohashNeighbors:
    """Tests for get_geohash_neighbors function."""
    
    def test_get_neighbors_valid_geohash(self):
        """Test getting neighbors for a valid geohash."""
        neighbors = get_geohash_neighbors("dr5r")
        
        # Should return a dict with 8 direction keys
        assert isinstance(neighbors, dict)
        assert len(neighbors) == 8
    
    def test_get_neighbors_empty_geohash(self):
        """Test getting neighbors for empty geohash returns empty dict."""
        neighbors = get_geohash_neighbors("")
        assert neighbors == {}
