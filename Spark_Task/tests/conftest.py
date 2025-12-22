"""
Pytest configuration and fixtures for Spark ETL tests.
"""
import os
import sys
import pytest
import tempfile
import shutil

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    spark = (
        SparkSession.builder
        .appName("SparkETLTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    
    yield spark
    
    spark.stop()


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test outputs."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def sample_restaurant_data(spark, temp_dir):
    """Create sample restaurant data for testing."""
    data = [
        (1, 10, "Test Cafe", 100, "US", "New York", 40.7128, -74.0060),
        (2, 20, "Test Bistro", 200, "FR", "Paris", 48.8566, 2.3522),
        (3, 30, "Test Diner", 300, "GB", "London", 51.5074, -0.1278),
        (4, 40, "Null Coords", 400, "US", "Chicago", None, None),  # Null coordinates
        (5, 50, "Invalid Lat", 500, "ES", "Barcelona", 91.0, 2.0),  # Invalid latitude
    ]
    
    columns = ["id", "franchise_id", "franchise_name", "restaurant_franchise_id", 
               "country", "city", "lat", "lng"]
    
    df = spark.createDataFrame(data, columns)
    
    csv_path = os.path.join(temp_dir, "restaurant_csv")
    df.write.option("header", "true").csv(csv_path)
    
    return csv_path, df


@pytest.fixture
def sample_weather_data(spark, temp_dir):
    """Create sample weather data for testing."""
    data = [
        (40.71, -74.01, "dr5r", 22.5, 65, 1013.2, "Sunny", "2024-01-01"),
        (48.86, 2.35, "u09t", 18.0, 70, 1015.0, "Cloudy", "2024-01-01"),
        (51.51, -0.13, "gcpv", 15.0, 80, 1010.5, "Rainy", "2024-01-01"),
        (41.88, -87.63, "dp3w", 20.0, 55, 1020.0, "Sunny", "2024-01-01"),
    ]
    
    columns = ["lat", "lng", "geohash", "temperature_c", "humidity_pct", 
               "pressure_mb", "condition", "observation_date"]
    
    df = spark.createDataFrame(data, columns)
    
    parquet_path = os.path.join(temp_dir, "weather_parquet")
    df.write.parquet(parquet_path)
    
    return parquet_path, df
