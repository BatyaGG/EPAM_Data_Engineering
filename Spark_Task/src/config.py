"""
Configuration module for Spark ETL job.
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ETLConfig:
    """Configuration for the Spark ETL job."""
    
    # Input paths
    restaurant_data_path: str = "restaurant_csv"
    weather_data_path: str = "weather_data/weather"
    
    # Output path
    output_path: str = "output/enriched_data"
    
    # Geohash configuration
    geohash_precision: int = 4
    
    # Geocoding configuration
    opencage_api_key: Optional[str] = None
    
    # Spark configuration
    spark_app_name: str = "Restaurant_Weather_ETL"
    spark_master: str = "local[*]"
    
    # Partitioning configuration
    partition_columns: tuple = ("country",)
    
    def __post_init__(self):
        """Load configuration from environment variables if not set."""
        if self.opencage_api_key is None:
            self.opencage_api_key = os.environ.get("OPENCAGE_API_KEY")


def get_config(base_path: str = ".") -> ETLConfig:
    """
    Get ETL configuration with resolved paths.
    
    Args:
        base_path: Base path for data files
        
    Returns:
        ETLConfig instance with resolved paths
    """
    return ETLConfig(
        restaurant_data_path=os.path.join(base_path, "restaurant_csv"),
        weather_data_path=os.path.join(base_path, "weather_parquet"),
        output_path=os.path.join(base_path, "output", "enriched_data"),
    )
