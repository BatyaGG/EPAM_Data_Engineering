"""
Main Spark ETL module for restaurant and weather data processing.
"""
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, udf, when, broadcast, first, coalesce, lit
)
from pyspark.sql.types import StringType, DoubleType, StructType, StructField

from .geohash_utils import generate_geohash
from .geocoding import OpenCageGeocoder
from .config import ETLConfig, get_config


class SparkETL:
    """Spark ETL job for processing restaurant and weather data."""
    
    def __init__(self, config: Optional[ETLConfig] = None, spark: Optional[SparkSession] = None):
        """
        Initialize the ETL job.
        
        Args:
            config: ETL configuration. Uses default if not provided.
            spark: SparkSession. Creates new session if not provided.
        """
        self.config = config or get_config()
        self.spark = spark or self._create_spark_session()
        
        # Register UDFs
        self._register_udfs()
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure SparkSession."""
        return (
            SparkSession.builder
            .appName(self.config.spark_app_name)
            .master(self.config.spark_master)
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate()
        )
    
    def _register_udfs(self):
        """Register User Defined Functions."""
        precision = self.config.geohash_precision
        geohash_func = lambda lat, lng: generate_geohash(lat, lng, precision)
        # Geohash UDF
        self.geohash_udf = udf(
            geohash_func,
            StringType()
        )
    
    def read_restaurant_data(self, path: Optional[str] = None) -> DataFrame:
        """
        Read restaurant data from CSV files.
        
        Args:
            path: Path to restaurant CSV files. Uses config path if not provided.
            
        Returns:
            DataFrame with restaurant data
        """
        data_path = path or self.config.restaurant_data_path
        
        df = (
            self.spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(data_path)
        )
        
        # Ensure correct column types
        df = df.select(
            col("id").cast("long"),
            col("franchise_id").cast("int"),
            col("franchise_name").cast("string"),
            col("restaurant_franchise_id").cast("int"),
            col("country").cast("string"),
            col("city").cast("string"),
            col("lat").cast("double"),
            col("lng").cast("double")
        )
        
        return df
    
    def read_weather_data(self, path: Optional[str] = None) -> DataFrame:
        """
        Read weather data from Parquet files.
        
        Args:
            path: Path to weather Parquet files. Uses config path if not provided.
            
        Returns:
            DataFrame with weather data
        """
        data_path = path or self.config.weather_data_path
        
        return self.spark.read.parquet(data_path)
    
    def identify_null_coordinates(self, df: DataFrame) -> DataFrame:
        """
        Identify records with null or invalid latitude/longitude values.
        
        Args:
            df: Restaurant DataFrame
            
        Returns:
            DataFrame with records that have null/invalid coordinates
        """
        return df.filter(
            col("lat").isNull() | 
            col("lng").isNull() |
            (col("lat") < -90) | (col("lat") > 90) |
            (col("lng") < -180) | (col("lng") > 180)
        )
    
    def geocode_missing_coordinates(
        self, 
        df: DataFrame, 
        api_key: Optional[str] = None
    ) -> DataFrame:
        """
        Fill in missing coordinates using OpenCage Geocoding API.
        
        For records with null lat/lng, geocodes based on city and country.
        
        Args:
            df: Restaurant DataFrame
            api_key: OpenCage API key. Uses config key if not provided.
            
        Returns:
            DataFrame with geocoded coordinates
        """
        key = api_key or self.config.opencage_api_key
        
        # Identify records needing geocoding
        needs_geocoding = df.filter(
            col("lat").isNull() | col("lng").isNull()
        )
        
        if needs_geocoding.count() == 0:
            return df
        
        if not key:
            print("Warning: No OpenCage API key provided. Skipping geocoding.")
            return df
        
        # Get unique city/country combinations
        unique_locations = (
            needs_geocoding
            .select("city", "country")
            .distinct()
            .collect()
        )
        
        # Geocode each unique location
        geocoder = OpenCageGeocoder(key)
        geocoded_data = []
        
        for row in unique_locations:
            lat, lng = geocoder.geocode(row.city, row.country)
            geocoded_data.append((row.city, row.country, lat, lng))
        
        # Create lookup DataFrame
        geocoded_schema = StructType([
            StructField("lookup_city", StringType(), True),
            StructField("lookup_country", StringType(), True),
            StructField("geocoded_lat", DoubleType(), True),
            StructField("geocoded_lng", DoubleType(), True),
        ])
        
        geocoded_df = self.spark.createDataFrame(geocoded_data, geocoded_schema)
        
        # Join and coalesce coordinates
        result = df.join(
            broadcast(geocoded_df),
            (df.city == geocoded_df.lookup_city) & (df.country == geocoded_df.lookup_country),
            "left"
        ).select(
            df.id,
            df.franchise_id,
            df.franchise_name,
            df.restaurant_franchise_id,
            df.country,
            df.city,
            coalesce(df.lat, geocoded_df.geocoded_lat).alias("lat"),
            coalesce(df.lng, geocoded_df.geocoded_lng).alias("lng")
        )
        
        return result
    
    def add_geohash_column(self, df: DataFrame) -> DataFrame:
        """
        Add geohash column based on latitude and longitude.
        
        Args:
            df: DataFrame with lat/lng columns
            
        Returns:
            DataFrame with added geohash column
        """
        return df.withColumn(
            "geohash",
            self.geohash_udf(col("lat"), col("lng"))
        )
    
    def join_with_weather(
        self, 
        restaurant_df: DataFrame, 
        weather_df: DataFrame
    ) -> DataFrame:
        """
        Left join restaurant data with weather data using geohash.
        
        Performs deduplication on weather data to avoid data multiplication.
        
        Args:
            restaurant_df: Restaurant DataFrame with geohash
            weather_df: Weather DataFrame with geohash
            
        Returns:
            Joined DataFrame with all fields from both datasets
        """
        # Deduplicate weather data by geohash to ensure idempotency
        # Take first record per geohash to avoid multiplication
        weather_dedupe = (
            weather_df
            .groupBy("geohash")
            .agg(*[
                first(c, ignorenulls=True).alias(f"weather_{c}") 
                for c in weather_df.columns if c != "geohash"
            ])
        )
        
        # Rename geohash in weather to avoid collision
        weather_dedupe = weather_dedupe.withColumnRenamed("geohash", "weather_geohash")
        
        # Left join on geohash
        result = restaurant_df.join(
            weather_dedupe,
            restaurant_df.geohash == weather_dedupe.weather_geohash,
            "left"
        ).drop("weather_geohash")
        
        return result
    
    def write_output(
        self, 
        df: DataFrame, 
        path: Optional[str] = None,
        mode: str = "overwrite"
    ):
        """
        Write enriched data to Parquet format with partitioning.
        
        Args:
            df: DataFrame to write
            path: Output path. Uses config path if not provided.
            mode: Write mode ('overwrite', 'append', etc.)
        """
        output_path = path or self.config.output_path
        partition_cols = list(self.config.partition_columns)
        
        (
            df.write
            .mode(mode)
            .partitionBy(*partition_cols)
            .parquet(output_path)
        )
        
        print(f"Data written to {output_path}")
    
    def run(
        self, 
        restaurant_path: Optional[str] = None,
        weather_path: Optional[str] = None,
        output_path: Optional[str] = None,
        geocode: bool = True
    ) -> DataFrame:
        """
        Run the complete ETL pipeline.
        
        Args:
            restaurant_path: Path to restaurant data
            weather_path: Path to weather data
            output_path: Path for output
            geocode: Whether to geocode missing coordinates
            
        Returns:
            Final enriched DataFrame
        """
        print("Starting ETL pipeline...")
        
        # Step 1: Read restaurant data
        print("Reading restaurant data...")
        restaurant_df = self.read_restaurant_data(restaurant_path)
        print(f"Loaded {restaurant_df.count()} restaurant records")
        
        # Step 2: Check for null coordinates
        null_coords = self.identify_null_coordinates(restaurant_df)
        null_count = null_coords.count()
        print(f"Found {null_count} records with null/invalid coordinates")
        
        # Step 3: Geocode missing coordinates (if enabled and API key available)
        if geocode and null_count > 0:
            print("Geocoding missing coordinates...")
            restaurant_df = self.geocode_missing_coordinates(restaurant_df)
        
        # Step 4: Add geohash column
        print("Generating geohashes...")
        restaurant_df = self.add_geohash_column(restaurant_df)
        
        # Step 5: Read and join weather data
        weather_path = weather_path or self.config.weather_data_path
        try:
            print("Reading weather data...")
            weather_df = self.read_weather_data(weather_path)
            
            # Add geohash to weather if not present
            if "geohash" not in weather_df.columns:
                weather_df = self.add_geohash_column(weather_df)
            
            print("Joining with weather data...")
            enriched_df = self.join_with_weather(restaurant_df, weather_df)
        except Exception as e:
            print(f"Warning: Could not read weather data ({e}). Proceeding without weather join.")
            enriched_df = restaurant_df
        
        # Step 6: Write output
        print("Writing output...")
        self.write_output(enriched_df, output_path)
        
        print("ETL pipeline completed successfully!")
        return enriched_df
    
    def stop(self):
        """Stop the SparkSession."""
        if self.spark:
            self.spark.stop()


def main():
    """Main entry point for running the ETL job."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Restaurant Weather ETL Job")
    parser.add_argument(
        "--restaurant-path", 
        default="restaurant_csv",
        help="Path to restaurant CSV data"
    )
    parser.add_argument(
        "--weather-path", 
        default="weather_parquet",
        help="Path to weather Parquet data"
    )
    parser.add_argument(
        "--output-path", 
        default="output/enriched_data",
        help="Path for output Parquet files"
    )
    parser.add_argument(
        "--no-geocode",
        action="store_true",
        help="Skip geocoding of missing coordinates"
    )
    
    args = parser.parse_args()
    
    config = ETLConfig(
        restaurant_data_path=args.restaurant_path,
        weather_data_path=args.weather_path,
        output_path=args.output_path,
    )
    
    etl = SparkETL(config)
    try:
        etl.run(geocode=not args.no_geocode)
    finally:
        etl.stop()


if __name__ == "__main__":
    main()
