#!/usr/bin/env python3
"""
Main entry point for running the Spark ETL job.

Usage:
    python run_etl.py [options]

Examples:
    # Run with default paths
    python run_etl.py

    # Run with custom paths
    python run_etl.py --restaurant-path data/restaurants --weather-path data/weather --output-path output/enriched

    # Run without geocoding
    python run_etl.py --no-geocode
"""
import os
import sys
import argparse

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.etl import SparkETL
from src.config import ETLConfig


def main():
    """Main entry point for the ETL job."""
    parser = argparse.ArgumentParser(
        description="Restaurant Weather ETL Job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python run_etl.py
    python run_etl.py --restaurant-path restaurant_csv --weather-path weather_parquet
    python run_etl.py --no-geocode --output-path custom_output
        """
    )
    
    parser.add_argument(
        "--restaurant-path",
        default="restaurant_csv",
        help="Path to restaurant CSV data (default: restaurant_csv)"
    )
    parser.add_argument(
        "--weather-path",
        default="weather_parquet",
        help="Path to weather Parquet data (default: weather_parquet)"
    )
    parser.add_argument(
        "--output-path",
        default="output/enriched_data",
        help="Path for output Parquet files (default: output/enriched_data)"
    )
    parser.add_argument(
        "--no-geocode",
        action="store_true",
        help="Skip geocoding of missing coordinates"
    )
    parser.add_argument(
        "--generate-weather",
        action="store_true",
        help="Generate sample weather data before running ETL"
    )
    parser.add_argument(
        "--weather-records",
        type=int,
        default=1000,
        help="Number of weather records to generate (default: 1000)"
    )
    
    args = parser.parse_args()
    
    # Get base path
    base_path = os.path.dirname(os.path.abspath(__file__))
    
    # Resolve paths
    restaurant_path = os.path.join(base_path, args.restaurant_path)
    weather_path = os.path.join(base_path, args.weather_path)
    output_path = os.path.join(base_path, args.output_path)
    
    # Generate weather data if requested
    if args.generate_weather:
        print("Generating sample weather data...")
        from scripts.generate_sample_weather import generate_weather_data
        from pyspark.sql import SparkSession
        
        spark = (
            SparkSession.builder
            .appName("WeatherDataGenerator")
            .master("local[*]")
            .getOrCreate()
        )
        
        generate_weather_data(spark, weather_path, args.weather_records)
        spark.stop()
        print("Weather data generated successfully!")
    
    # Create configuration
    config = ETLConfig(
        restaurant_data_path=restaurant_path,
        weather_data_path=weather_path,
        output_path=output_path,
    )
    
    # Run ETL
    print("=" * 60)
    print("SPARK ETL JOB - Restaurant Weather Data Processing")
    print("=" * 60)
    print(f"Restaurant data: {restaurant_path}")
    print(f"Weather data: {weather_path}")
    print(f"Output path: {output_path}")
    print(f"Geocoding: {'Disabled' if args.no_geocode else 'Enabled'}")
    print("=" * 60)
    
    etl = SparkETL(config)
    try:
        result_df = etl.run(geocode=not args.no_geocode)
        
        print("\n" + "=" * 60)
        print("ETL JOB COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"Total records processed: {result_df.count()}")
        print(f"Output location: {output_path}")
        print("\nSample of enriched data:")
        result_df.show(5, truncate=False)
        
    except Exception as e:
        print(f"\nETL JOB FAILED: {e}")
        raise
    finally:
        etl.stop()


if __name__ == "__main__":
    main()
