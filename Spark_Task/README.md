# Spark ETL Job - Restaurant Weather Data Processing

[![GitHub Repository](https://img.shields.io/badge/GitHub-Repository-blue?logo=github)](https://github.com/BatyaGG/EPAM_Data_Engineering)

**Repository:** [https://github.com/BatyaGG/EPAM_Data_Engineering](https://github.com/BatyaGG/EPAM_Data_Engineering)

## Overview

This project implements a Spark ETL (Extract, Transform, Load) job that processes restaurant data, enriches it with geolocation information, and joins it with weather data. The solution handles data quality issues, generates geohashes for spatial matching, and produces partitioned Parquet output.

## 🚀 Quick Start

### Option 1: Jupyter Notebook (Recommended)

The complete ETL pipeline with step-by-step execution, documentation, and outputs is available in the Jupyter notebook:

```bash
cd Spark_Task
jupyter notebook Spark_ETL_Task.ipynb
```

The notebook includes:
- ✅ Data loading and exploration
- ✅ Null/invalid coordinate detection
- ✅ OpenCage Geocoding API integration
- ✅ 4-character geohash generation
- ✅ Weather data join with deduplication
- ✅ Parquet output with partitioning
- ✅ Summary statistics and verification

### Option 2: Command Line

```bash
cd Spark_Task
pip install -r requirements.txt
python run_etl.py
```

## Project Structure

```
Spark_Task/
├── README.md                          # This documentation file
├── requirements.txt                   # Python dependencies
├── run_etl.py                        # Main entry point script
├── Spark_ETL_Task.ipynb              # Jupyter notebook with full ETL implementation
├── restaurant_csv/                    # Input restaurant data (CSV)
├── weather_data/weather/              # Input weather data (Parquet, partitioned by year/month/day)
├── weather_zips/                      # Original weather data zip files
├── output/                            # Output directory for enriched data
├── src/
│   ├── __init__.py
│   ├── config.py                     # Configuration management
│   ├── etl.py                        # Main ETL logic
│   ├── geocoding.py                  # OpenCage Geocoding API integration
│   └── geohash_utils.py              # Geohash generation utilities
├── scripts/
│   ├── __init__.py
│   └── generate_sample_weather.py    # Sample weather data generator
└── tests/
    ├── __init__.py
    ├── conftest.py                   # Pytest fixtures
    ├── test_etl.py                   # ETL tests
    ├── test_geocoding.py             # Geocoding tests
    └── test_geohash_utils.py         # Geohash utility tests
```

## Prerequisites

### 1. Install Apache Spark

#### Option A: Using Homebrew (macOS)
```bash
brew install apache-spark
```

#### Option B: Manual Installation
1. Download Spark from https://spark.apache.org/downloads.html
2. Extract and set environment variables:
```bash
export SPARK_HOME=/path/to/spark
export PATH=$PATH:$SPARK_HOME/bin
```

#### Option C: Using Docker
```bash
docker run -it --rm -v $(pwd):/app -w /app apache/spark:3.5.0 /bin/bash
```

### 2. Install Python Dependencies
```bash
cd Spark_Task
pip install -r requirements.txt
```

### 3. Set Up OpenCage API Key (Optional)
To enable geocoding of missing coordinates:
1. Sign up at https://opencagedata.com/
2. Get your free API key
3. Set the environment variable:
```bash
export OPENCAGE_API_KEY=your_api_key_here
```

## Data Schema

### Restaurant Data (Input)
| Column | Type | Description |
|--------|------|-------------|
| id | Long | Unique restaurant identifier |
| franchise_id | Integer | Franchise identifier |
| franchise_name | String | Name of the franchise |
| restaurant_franchise_id | Integer | Restaurant franchise ID |
| country | String | Country code (US, GB, FR, etc.) |
| city | String | City name |
| lat | Double | Latitude coordinate |
| lng | Double | Longitude coordinate |

### Weather Data (Input)
| Column | Type | Description |
|--------|------|-------------|
| lat | Double | Latitude coordinate |
| lng | Double | Longitude coordinate |
| avg_tmpr_f | Double | Average temperature in Fahrenheit |
| avg_tmpr_c | Double | Average temperature in Celsius |
| wthr_date | String | Weather observation date |
| year | Integer | Year (partition column) |
| month | Integer | Month (partition column) |
| day | Integer | Day (partition column) |

### Enriched Data (Output)
All columns from restaurant data, plus:
- `geohash`: 4-character geohash generated from coordinates
- `weather_lat`: Weather station latitude
- `weather_lng`: Weather station longitude
- `weather_avg_tmpr_f`: Average temperature in Fahrenheit
- `weather_avg_tmpr_c`: Average temperature in Celsius
- `weather_wthr_date`: Weather observation date

## Usage

### Jupyter Notebook (Primary Method)

Open and run the **`Spark_ETL_Task.ipynb`** notebook for an interactive, documented ETL experience:

```bash
jupyter notebook Spark_ETL_Task.ipynb
```

### Command Line (Alternative)

1. **Run the ETL job:**
```bash
python run_etl.py
```

2. **With weather data generation:**
```bash
python run_etl.py --generate-weather
```

### Command Line Options

```bash
python run_etl.py [OPTIONS]

Options:
  --restaurant-path PATH   Path to restaurant CSV data (default: restaurant_csv)
  --weather-path PATH      Path to weather Parquet data (default: weather_parquet)
  --output-path PATH       Path for output Parquet files (default: output/enriched_data)
  --no-geocode            Skip geocoding of missing coordinates
  --generate-weather      Generate sample weather data before running ETL
  --weather-records NUM   Number of weather records to generate (default: 1000)
```

### Examples

```bash
# Run with default settings
python run_etl.py

# Generate weather data and run ETL
python run_etl.py --generate-weather

# Run without geocoding (faster, no API needed)
python run_etl.py --no-geocode

# Custom paths
python run_etl.py --restaurant-path data/restaurants --output-path output/final
```

## ETL Pipeline Steps

### Step 1: Read Restaurant Data
- Reads CSV files from the input directory
- Infers schema and casts columns to appropriate types

### Step 2: Identify Invalid Coordinates
- Checks for NULL latitude/longitude values
- Validates coordinate ranges (-90 to 90 for lat, -180 to 180 for lng)

**Screenshot placeholder: Records with null coordinates**
```
+----+-----------+-----------+--------+------+----------+----+----+
|  id|franchise_id|city       |country |lat   |lng       |
+----+-----------+-----------+--------+------+----------+----+
|   4|         40|Chicago    |US      |null  |null      |
|   5|         50|Barcelona  |ES      |91.0  |2.0       |
+----+-----------+-----------+--------+------+----------+----+
```

### Step 3: Geocode Missing Coordinates (Optional)
- Uses OpenCage Geocoding API to resolve coordinates
- Caches results to minimize API calls
- Rate-limited to respect API limits

### Step 4: Generate Geohash
- Creates 4-character geohash from latitude/longitude
- Uses the `python-geohash` library
- Handles null/invalid coordinates gracefully

**Screenshot placeholder: Data with geohash column**
```
+----+-----------+------+--------+-------+
|  id|city       |lat   |lng     |geohash|
+----+-----------+------+--------+-------+
|   1|New York   |40.71 |-74.01  |dr5r   |
|   2|Paris      |48.86 |2.35    |u09t   |
|   3|London     |51.51 |-0.13   |gcpv   |
+----+-----------+------+--------+-------+
```

### Step 5: Join with Weather Data
- Left joins restaurant data with weather data on geohash
- Deduplicates weather data to prevent data multiplication
- Ensures idempotency (same input → same output)

### Step 6: Write Enriched Data
- Outputs data in Parquet format
- Partitions by country for efficient querying
- Uses Snappy compression

**Screenshot placeholder: Output directory structure**
```
output/enriched_data/
├── country=US/
│   └── part-00000-*.parquet
├── country=GB/
│   └── part-00000-*.parquet
├── country=FR/
│   └── part-00000-*.parquet
└── _SUCCESS
```

## Running Tests

The project includes comprehensive unit tests for all ETL components.

```bash
# Run all tests with verbose output
cd Spark_Task
python3 -m pytest tests/ -v

# Run with coverage report
python3 -m pytest tests/ -v --cov=src --cov-report=html

# Run specific test files
python3 -m pytest tests/test_geohash_utils.py -v   # Geohash tests
python3 -m pytest tests/test_geocoding.py -v       # Geocoding tests
python3 -m pytest tests/test_etl.py -v             # ETL pipeline tests
```

### Test Coverage

| Test File | Description | Tests |
|-----------|-------------|-------|
| `test_geohash_utils.py` | Geohash generation functions | 14 tests |
| `test_geocoding.py` | OpenCage API integration | 10 tests |
| `test_etl.py` | Spark ETL pipeline | 10 tests |

**Expected Output:**
```
========================= test session starts ==========================
tests/test_etl.py::TestSparkETL::test_read_restaurant_data PASSED
tests/test_etl.py::TestSparkETL::test_identify_null_coordinates PASSED
tests/test_etl.py::TestSparkETL::test_add_geohash_column PASSED
tests/test_etl.py::TestSparkETL::test_geohash_is_4_characters PASSED
tests/test_etl.py::TestSparkETL::test_join_with_weather_no_multiplication PASSED
tests/test_geohash_utils.py::TestGenerateGeohash::test_valid_coordinates PASSED
tests/test_geohash_utils.py::TestGenerateGeohash::test_null_coordinates PASSED
tests/test_geocoding.py::TestOpenCageGeocoder::test_geocode_success PASSED
...
========================= 34 passed, 2 skipped ==========================
```

## Key Design Decisions

### 1. Geohash Precision
- Uses 4-character geohashes (~39km x 20km cells)
- Balances precision with join efficiency
- Allows matching nearby weather stations to restaurants

### 2. Data Multiplication Prevention
- Weather data is deduplicated by geohash before joining
- Uses `first()` aggregation to select one weather record per geohash
- Ensures restaurant count remains unchanged after join

### 3. Idempotency
- Uses `overwrite` mode for output
- Same input data always produces same output
- Safe to re-run without duplicating data

### 4. Partitioning Strategy
- Partitions output by country
- Improves query performance for country-specific analysis
- Reduces data scanning for filtered queries

### 5. Error Handling
- Graceful handling of missing weather data
- Null geohashes for invalid coordinates
- Continues processing even if geocoding fails

## Troubleshooting

### Common Issues

1. **"Could not read weather data"**
   - Generate weather data first: `python run_etl.py --generate-weather`

2. **"No OpenCage API key provided"**
   - Set the environment variable: `export OPENCAGE_API_KEY=your_key`
   - Or run with `--no-geocode` flag

3. **"Java not found"**
   - Install Java: `brew install openjdk@11`
   - Set JAVA_HOME: `export JAVA_HOME=$(/usr/libexec/java_home)`

4. **Out of Memory**
   - Increase Spark memory: Add `--driver-memory 4g` to spark-submit

## Performance Considerations

- **Small datasets**: Local mode (`local[*]`) is sufficient
- **Large datasets**: Consider using a Spark cluster
- **Geocoding**: Use batch processing with caching to minimize API calls
- **Joins**: Broadcast weather data for better performance (assumes weather dataset is smaller)

## ETL Results Summary

After running the ETL pipeline:

| Metric | Value |
|--------|-------|
| Restaurant records | 1,997 |
| Weather records | 112,394,743 |
| Unique restaurant geohashes | 692 |
| Unique weather geohashes | 346,655 |
| Matching geohashes | 685 |
| Records with weather data | 1,875 (93.9%) |
| Output format | Parquet (Snappy) |
| Partitioning | By country |

## Future Improvements

1. Add support for incremental processing
2. Implement data validation rules
3. Add monitoring and alerting
4. Support for real-time weather data feeds
5. Add data lineage tracking

## Author

**Batyrkhan Ganiev** - EPAM Data Engineering Program

## License

This project is part of the EPAM Data Engineering training program.

---

📓 **Full implementation with outputs:** See [Spark_ETL_Task.ipynb](Spark_ETL_Task.ipynb)
