"""
Script to generate sample weather data for testing.

This creates synthetic weather data with geohashes matching the restaurant dataset.
"""
import os
import sys
import random
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)

try:
    import geohash as gh
except ImportError:
    print("Installing python-geohash...")
    os.system("pip install python-geohash")
    import geohash as gh


def generate_weather_data(spark: SparkSession, output_path: str, num_records: int = 1000):
    """
    Generate sample weather data with geohashes.
    
    Args:
        spark: SparkSession
        output_path: Path to write weather Parquet files
        num_records: Number of weather records to generate
    """
    
    # Sample coordinates covering major cities from restaurant data
    sample_locations = [
        # US locations
        (45.533, 9.171, "Milan", "IT"),
        (48.873, 2.305, "Paris", "FR"),
        (52.392, 4.911, "Amsterdam", "NL"),
        (51.502, 0.000, "London", "GB"),
        (40.115, -83.015, "Columbus", "US"),
        (33.382, -103.395, "Tatum", "US"),
        (36.684, -121.792, "Marina", "US"),
        (41.324, -92.646, "Oskaloosa", "US"),
        (48.163, 16.340, "Vienna", "AT"),
        (41.396, 2.163, "Barcelona", "ES"),
        (41.387, 2.174, "Barcelona", "ES"),
        (39.631, -79.956, "Morgantown", "US"),
        (34.701, -76.747, "Atlantic Beach", "US"),
        (48.213, 16.357, "Vienna", "AT"),
        (51.514, -0.177, "London", "GB"),
        (51.525, -0.077, "London", "GB"),
        (43.479, -89.769, "Baraboo", "US"),
        (42.200, -71.841, "Auburn", "US"),
        (52.358, 4.845, "Amsterdam", "NL"),
        (33.621, -112.186, "Glendale", "US"),
        (35.868, -83.551, "Sevierville", "US"),
        (29.753, -95.361, "Houston", "US"),
        (32.776, -117.252, "San Diego", "US"),
        (40.758, -73.986, "New York", "US"),
        (38.880, -104.868, "Colorado Springs", "US"),
    ]
    
    # Weather conditions
    conditions = ["Sunny", "Cloudy", "Rainy", "Partly Cloudy", "Foggy", "Snowy", "Windy"]
    
    # Generate records
    weather_records = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(num_records):
        lat, lng, city, country = random.choice(sample_locations)
        
        # Add some randomness to coordinates
        lat_offset = random.uniform(-0.1, 0.1)
        lng_offset = random.uniform(-0.1, 0.1)
        
        final_lat = lat + lat_offset
        final_lng = lng + lng_offset
        
        # Generate 4-character geohash
        geohash_4 = gh.encode(final_lat, final_lng, precision=4)
        
        # Random weather data
        record = {
            "lat": final_lat,
            "lng": final_lng,
            "geohash": geohash_4,
            "temperature_c": round(random.uniform(-10, 35), 1),
            "humidity_pct": random.randint(20, 100),
            "pressure_mb": round(random.uniform(980, 1030), 1),
            "wind_speed_kmh": round(random.uniform(0, 50), 1),
            "wind_direction": random.choice(["N", "NE", "E", "SE", "S", "SW", "W", "NW"]),
            "condition": random.choice(conditions),
            "observation_date": (base_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
            "city": city,
            "country": country,
        }
        weather_records.append(record)
    
    # Define schema
    schema = StructType([
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("geohash", StringType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity_pct", IntegerType(), True),
        StructField("pressure_mb", DoubleType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("wind_direction", StringType(), True),
        StructField("condition", StringType(), True),
        StructField("observation_date", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
    ])
    
    # Create DataFrame
    weather_df = spark.createDataFrame(weather_records, schema)
    
    # Write as Parquet
    weather_df.write.mode("overwrite").parquet(output_path)
    
    print(f"Generated {num_records} weather records at {output_path}")
    print(f"Sample geohashes: {[r['geohash'] for r in weather_records[:5]]}")
    
    return weather_df


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate sample weather data")
    parser.add_argument(
        "--output", 
        default="weather_parquet",
        help="Output path for weather data"
    )
    parser.add_argument(
        "--num-records",
        type=int,
        default=1000,
        help="Number of weather records to generate"
    )
    
    args = parser.parse_args()
    
    spark = (
        SparkSession.builder
        .appName("WeatherDataGenerator")
        .master("local[*]")
        .getOrCreate()
    )
    
    try:
        generate_weather_data(spark, args.output, args.num_records)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
