"""
Tests for Spark ETL functionality.
"""
import pytest
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql.functions import col
from src.etl import SparkETL
from src.config import ETLConfig


class TestSparkETL:
    """Tests for SparkETL class."""
    
    def test_read_restaurant_data(self, spark, sample_restaurant_data):
        """Test reading restaurant data from CSV."""
        csv_path, expected_df = sample_restaurant_data
        
        config = ETLConfig(restaurant_data_path=csv_path)
        etl = SparkETL(config, spark)
        
        result_df = etl.read_restaurant_data()
        
        assert result_df.count() == 5
        assert "id" in result_df.columns
        assert "lat" in result_df.columns
        assert "lng" in result_df.columns
        assert "city" in result_df.columns
    
    def test_identify_null_coordinates(self, spark, sample_restaurant_data):
        """Test identifying records with null coordinates."""
        csv_path, _ = sample_restaurant_data
        
        config = ETLConfig(restaurant_data_path=csv_path)
        etl = SparkETL(config, spark)
        
        restaurant_df = etl.read_restaurant_data()
        null_df = etl.identify_null_coordinates(restaurant_df)
        
        # Should find records with null coords and invalid latitude
        assert null_df.count() == 2
    
    def test_add_geohash_column(self, spark, sample_restaurant_data):
        """Test adding geohash column to DataFrame."""
        csv_path, _ = sample_restaurant_data
        
        config = ETLConfig(restaurant_data_path=csv_path)
        etl = SparkETL(config, spark)
        
        restaurant_df = etl.read_restaurant_data()
        result_df = etl.add_geohash_column(restaurant_df)
        
        assert "geohash" in result_df.columns
        
        # Check valid coordinates get geohash
        valid_row = result_df.filter(col("id") == 1).first()
        assert valid_row.geohash is not None
        assert len(valid_row.geohash) == 4
        
        # Check null coordinates get null geohash
        null_row = result_df.filter(col("id") == 4).first()
        assert null_row.geohash is None
    
    def test_geohash_is_4_characters(self, spark, sample_restaurant_data):
        """Test that generated geohashes are exactly 4 characters."""
        csv_path, _ = sample_restaurant_data
        
        config = ETLConfig(restaurant_data_path=csv_path, geohash_precision=4)
        etl = SparkETL(config, spark)
        
        restaurant_df = etl.read_restaurant_data()
        result_df = etl.add_geohash_column(restaurant_df)
        
        # Check all non-null geohashes are 4 characters
        valid_geohashes = (
            result_df
            .filter(col("geohash").isNotNull())
            .select("geohash")
            .collect()
        )
        
        for row in valid_geohashes:
            assert len(row.geohash) == 4
    
    def test_join_with_weather_no_multiplication(self, spark, sample_restaurant_data, sample_weather_data):
        """Test that left join doesn't cause data multiplication."""
        csv_path, _ = sample_restaurant_data
        weather_path, _ = sample_weather_data
        
        config = ETLConfig(
            restaurant_data_path=csv_path,
            weather_data_path=weather_path
        )
        etl = SparkETL(config, spark)
        
        restaurant_df = etl.read_restaurant_data()
        restaurant_df = etl.add_geohash_column(restaurant_df)
        
        weather_df = etl.read_weather_data()
        
        result_df = etl.join_with_weather(restaurant_df, weather_df)
        
        # Should have same number of records as restaurants (left join)
        assert result_df.count() == restaurant_df.count()
    
    def test_write_output_creates_parquet(self, spark, sample_restaurant_data, temp_dir):
        """Test that output is written in Parquet format."""
        csv_path, _ = sample_restaurant_data
        output_path = os.path.join(temp_dir, "output")
        
        config = ETLConfig(
            restaurant_data_path=csv_path,
            output_path=output_path
        )
        etl = SparkETL(config, spark)
        
        restaurant_df = etl.read_restaurant_data()
        restaurant_df = etl.add_geohash_column(restaurant_df)
        
        etl.write_output(restaurant_df, output_path)
        
        # Check output directory exists
        assert os.path.exists(output_path)
        
        # Check we can read it back
        read_df = spark.read.parquet(output_path)
        assert read_df.count() == restaurant_df.count()
    
    def test_write_output_with_partitioning(self, spark, sample_restaurant_data, temp_dir):
        """Test that output is partitioned correctly."""
        csv_path, _ = sample_restaurant_data
        output_path = os.path.join(temp_dir, "partitioned_output")
        
        config = ETLConfig(
            restaurant_data_path=csv_path,
            output_path=output_path,
            partition_columns=("country",)
        )
        etl = SparkETL(config, spark)
        
        restaurant_df = etl.read_restaurant_data()
        restaurant_df = etl.add_geohash_column(restaurant_df)
        
        etl.write_output(restaurant_df, output_path)
        
        # Check partition directories exist
        partition_dirs = [d for d in os.listdir(output_path) if d.startswith("country=")]
        assert len(partition_dirs) > 0


class TestETLIdempotency:
    """Tests for ETL job idempotency."""
    
    def test_multiple_runs_same_result(self, spark, sample_restaurant_data, temp_dir):
        """Test that running ETL multiple times produces same result."""
        csv_path, _ = sample_restaurant_data
        output_path = os.path.join(temp_dir, "idempotent_output")
        
        config = ETLConfig(
            restaurant_data_path=csv_path,
            output_path=output_path
        )
        
        # First run
        etl1 = SparkETL(config, spark)
        restaurant_df1 = etl1.read_restaurant_data()
        restaurant_df1 = etl1.add_geohash_column(restaurant_df1)
        etl1.write_output(restaurant_df1, output_path)
        
        result1 = spark.read.parquet(output_path)
        count1 = result1.count()
        
        # Second run (overwrite)
        etl2 = SparkETL(config, spark)
        restaurant_df2 = etl2.read_restaurant_data()
        restaurant_df2 = etl2.add_geohash_column(restaurant_df2)
        etl2.write_output(restaurant_df2, output_path)
        
        result2 = spark.read.parquet(output_path)
        count2 = result2.count()
        
        assert count1 == count2


class TestETLDataQuality:
    """Tests for data quality checks."""
    
    def test_all_valid_coords_have_geohash(self, spark, sample_restaurant_data):
        """Test that all records with valid coordinates have a geohash."""
        csv_path, _ = sample_restaurant_data
        
        config = ETLConfig(restaurant_data_path=csv_path)
        etl = SparkETL(config, spark)
        
        restaurant_df = etl.read_restaurant_data()
        result_df = etl.add_geohash_column(restaurant_df)
        
        # Records with valid coordinates should have geohash
        valid_coords = result_df.filter(
            col("lat").isNotNull() & 
            col("lng").isNotNull() &
            (col("lat") >= -90) & (col("lat") <= 90) &
            (col("lng") >= -180) & (col("lng") <= 180)
        )
        
        null_geohash = valid_coords.filter(col("geohash").isNull())
        assert null_geohash.count() == 0
    
    def test_weather_join_preserves_restaurant_columns(self, spark, sample_restaurant_data, sample_weather_data):
        """Test that weather join preserves all restaurant columns."""
        csv_path, _ = sample_restaurant_data
        weather_path, _ = sample_weather_data
        
        config = ETLConfig(
            restaurant_data_path=csv_path,
            weather_data_path=weather_path
        )
        etl = SparkETL(config, spark)
        
        restaurant_df = etl.read_restaurant_data()
        original_columns = set(restaurant_df.columns)
        
        restaurant_df = etl.add_geohash_column(restaurant_df)
        weather_df = etl.read_weather_data()
        result_df = etl.join_with_weather(restaurant_df, weather_df)
        
        result_columns = set(result_df.columns)
        
        # All original columns plus geohash should be preserved
        expected_columns = original_columns | {"geohash"}
        assert expected_columns.issubset(result_columns)
