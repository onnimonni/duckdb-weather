-- ============================================================
-- Weather Data Pipeline: NOAA GFS → H3 Parquet (Nordics)
-- ============================================================
-- Usage: duckdb -unsigned -f scripts/weather_pipeline_nordics.sql
--
-- This pipeline:
-- 1. Fetches Nordic region GFS forecast data from NOAA via HTTP
-- 2. Converts to H3-indexed parquet (resolution 5 = ~8.5km)
-- 3. Saves to local file (can be uploaded to WebDAV separately)
--
-- Nordic region: 54°N-72°N, -25°W-32°E
-- Covers: Finland, Sweden, Norway, Denmark, Iceland, Baltics, NW Russia
-- ============================================================

-- Install required extensions
INSTALL h3 FROM community;
INSTALL httpfs;

LOAD h3;
LOAD httpfs;

-- Load weather extension
LOAD './build/release/extension/weather/weather.duckdb_extension';

-- ============================================================
-- Configuration
-- ============================================================

-- H3 resolution for spatial indexing (5 = ~8.5km edge length)
SET VARIABLE h3_res = 5;

-- Nordic bounding box
SET VARIABLE lat_min = 54;   -- Southern Denmark
SET VARIABLE lat_max = 72;   -- Northern Norway/Finland
SET VARIABLE lon_min = -25;  -- Iceland
SET VARIABLE lon_max = 32;   -- Eastern Finland

-- Output file
SET VARIABLE output_file = 'nordics_weather.parquet';

SELECT 'Nordic Weather Pipeline' as status;
SELECT 'Region: ' || getvariable('lat_min') || '°N to ' || getvariable('lat_max') || '°N, ' ||
       getvariable('lon_min') || '°E to ' || getvariable('lon_max') || '°E' as info;
SELECT 'Run: ' || strftime(now() AT TIME ZONE 'UTC', '%Y-%m-%d') || ' 00Z' as info;

-- ============================================================
-- Process Nordic region and save to parquet
-- ============================================================

COPY (
    SELECT
        h3_latlng_to_cell(latitude, longitude, getvariable('h3_res'))::UBIGINT as h3_index,
        (strptime(run_date, '%Y%m%d') + interval '1 hour' * run_hour)::TIMESTAMPTZ as model_run_at,
        (strptime(run_date, '%Y%m%d') + interval '1 hour' * (run_hour + forecast_hour))::TIMESTAMPTZ as forecast_at,
        -- Temperature: 2m above ground (K → C)
        (MAX(CASE WHEN variable = 'temperature' AND level = '2m'
            THEN value - 273.15 END))::REAL as temperature_celsius,
        -- Humidity: 2m above ground (%)
        (MAX(CASE WHEN variable = 'humidity' AND level = '2m'
            THEN value END))::REAL as humidity_percentage,
        -- Wind gust: surface (m/s)
        (MAX(CASE WHEN variable = 'gust' AND level = 'surface'
            THEN value END))::REAL as wind_gust_ms,
        -- Wind speed: derived from U,V at 10m (m/s)
        (sqrt(
            pow(MAX(CASE WHEN variable = 'wind_u' AND level = '10m' THEN value END), 2) +
            pow(MAX(CASE WHEN variable = 'wind_v' AND level = '10m' THEN value END), 2)
        ))::REAL as wind_speed_ms,
        -- Wind direction: meteorological (where wind comes FROM, degrees)
        ((270 - degrees(atan2(
            MAX(CASE WHEN variable = 'wind_v' AND level = '10m' THEN value END),
            MAX(CASE WHEN variable = 'wind_u' AND level = '10m' THEN value END)
        )) + 360) % 360)::REAL as wind_direction_deg,
        -- Precipitation: surface (kg/m²)
        (MAX(CASE WHEN variable = 'precipitation' AND level = 'surface'
            THEN value END))::REAL as precipitation_kgm2,
        -- Cloud cover: entire atmosphere (%)
        (MAX(CASE WHEN variable = 'clouds' AND level = 'atmosphere'
            THEN value END))::REAL as cloud_cover_percentage,
        -- Pressure: mean sea level (Pa → hPa)
        (MAX(CASE WHEN variable = 'pressure' AND level = 'msl'
            THEN value / 100.0 END))::REAL as sea_level_pressure_hpa
    FROM noaa_gfs_forecast_api()
    WHERE run_date = strftime(now() AT TIME ZONE 'UTC', '%Y%m%d')
      AND run_hour = 0
      AND forecast_hour IN (
          -- Next 24h: every 3h
          0, 3, 6, 9, 12, 15, 18, 21, 24,
          -- Days 2-5: every 6h
          30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 96, 102, 108, 114, 120,
          -- Days 6-10: every 12h
          132, 144, 156, 168, 180, 192, 204, 216, 228, 240
      )
      AND variable IN ('temperature', 'humidity', 'wind_u', 'wind_v', 'precipitation', 'gust', 'clouds', 'pressure')
      AND level IN ('2m', '10m', 'surface', 'atmosphere', 'msl')
      AND latitude BETWEEN getvariable('lat_min') AND getvariable('lat_max')
      AND longitude BETWEEN getvariable('lon_min') AND getvariable('lon_max')
    GROUP BY h3_index, model_run_at, forecast_at
    ORDER BY h3_index, forecast_at
) TO 'nordics_weather.parquet' (FORMAT PARQUET, PARQUET_VERSION v2, COMPRESSION ZSTD, COMPRESSION_LEVEL 20);

SELECT 'Pipeline complete!' as status;
SELECT 'Output: nordics_weather.parquet' as info;

-- ============================================================
-- Quick verification query
-- ============================================================

SELECT 'Verification - row count and forecast range:' as info;
SELECT
    count(*) as total_rows,
    count(DISTINCT h3_index) as unique_locations,
    min(forecast_at) as first_forecast,
    max(forecast_at) as last_forecast
FROM read_parquet('nordics_weather.parquet');

-- ============================================================
-- Example query (Helsinki)
-- ============================================================

SELECT 'Example - Helsinki forecast:' as info;
WITH target AS (SELECT h3_latlng_to_cell(60.17, 24.94, 5)::UBIGINT as h3_idx)
SELECT
    strftime(forecast_at, '%a %d %b %H:%M') as forecast,
    round(temperature_celsius, 1) as "°C",
    round(humidity_percentage, 0)::INT as "RH%",
    round(wind_speed_ms, 1) as "Wind m/s",
    round(cloud_cover_percentage, 0)::INT as "Cloud%"
FROM read_parquet('nordics_weather.parquet'), target
WHERE h3_index IN (SELECT unnest(h3_grid_disk(h3_idx, 1)) FROM target)
  AND forecast_at >= now()
ORDER BY forecast_at
LIMIT 10;
