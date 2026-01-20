-- Weather Data Ingestion Pipeline
-- Converts GRIB2 files to H3-indexed Parquet for read_weather() queries
--
-- Usage:
--   duckdb -unsigned -init ingest_weather.sql
--
-- Prerequisites:
--   - Weather extension built and loaded
--   - GRIB2 files downloaded to /tmp/gfs_tampere/
--   - Target: WebDAV storage or local directory

-- Install required extensions
INSTALL h3 FROM community;
LOAD h3;

INSTALL httpfs;
LOAD httpfs;

-- Load weather extension (adjust path as needed)
LOAD './build/release/weather.duckdb_extension';

-- Configuration
SET VARIABLE grib_dir = '/tmp/gfs_tampere';
SET VARIABLE output_dir = '/tmp/weather_parquet';
SET VARIABLE h3_resolution = 5;  -- ~8.5km, matches GFS 0.25° resolution

-- Create output directory structure
-- Note: In production, this would be s3:// or webdav:// path

-- Main ingestion query: GRIB2 → H3-indexed Parquet
COPY (
    WITH raw_weather AS (
        SELECT
            latitude,
            longitude,
            value,
            discipline,
            surface,
            parameter,
            forecast_time,
            surface_value,
            message_index
        FROM read_grib(getvariable('grib_dir') || '/*.grib2')
        WHERE discipline = 'Meteorological'
    ),
    with_h3 AS (
        SELECT
            -- H3 spatial index (primary key for queries)
            h3_latlng_to_cell(latitude, longitude, getvariable('h3_resolution'))::UBIGINT as h3_index,

            -- Original coordinates (for precise location)
            latitude,
            longitude,

            -- Weather data
            CASE
                WHEN parameter = 'Temperature' THEN value - 273.15  -- K to °C
                ELSE value
            END as value,

            -- Metadata
            parameter,
            surface,
            forecast_time,

            -- Calculated fields
            CASE
                WHEN parameter = 'Temperature' THEN 'celsius'
                WHEN parameter IN ('U_Wind', 'V_Wind', 'Wind_Speed', 'Wind_Gust') THEN 'm/s'
                WHEN parameter = 'Pressure' THEN 'Pa'
                WHEN parameter = 'Relative_Humidity' THEN '%'
                WHEN parameter = 'Total_Precip' THEN 'mm'
                WHEN parameter = 'Cloud_Cover' THEN '%'
                ELSE 'raw'
            END as unit
        FROM raw_weather
    )
    SELECT * FROM with_h3
    ORDER BY h3_index, forecast_time
) TO '/tmp/weather_parquet/weather_h3.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);

-- Verify output
SELECT
    count(*) as total_rows,
    count(DISTINCT h3_index) as unique_h3_cells,
    count(DISTINCT parameter) as parameters,
    min(forecast_time) as min_forecast_hour,
    max(forecast_time) as max_forecast_hour
FROM read_parquet('/tmp/weather_parquet/weather_h3.parquet');

-- Show sample data
SELECT
    h3_cell_to_lat(h3_index) as lat,
    h3_cell_to_lng(h3_index) as lng,
    parameter,
    round(value, 1) as value,
    unit,
    forecast_time as hours
FROM read_parquet('/tmp/weather_parquet/weather_h3.parquet')
WHERE parameter = 'Temperature'
  AND h3_cell_to_lat(h3_index) BETWEEN 61 AND 62
LIMIT 10;

SELECT 'Ingestion complete!' as status;
