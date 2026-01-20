-- ============================================================
-- Weather Data Pipeline: NOAA GFS → H3 Parquet → WebDAV
-- ============================================================
-- Usage: duckdb -unsigned -f weather_pipeline.sql
--
-- This pipeline:
-- 1. Fetches global GFS forecast data directly from NOAA via HTTP
-- 2. Converts to H3-indexed parquet (resolution 5 = ~8.5km)
-- 3. Uploads to Hetzner WebDAV storage
--
-- Uses noaa_gfs_forecast_api() with SQL filter pushdown
-- ============================================================

-- Install required extensions
INSTALL h3 FROM community;
INSTALL httpfs;
INSTALL webdavfs FROM community;

LOAD h3;
LOAD httpfs;
LOAD webdavfs;

-- Load weather extension
LOAD './build/release/extension/weather/weather.duckdb_extension';

-- ============================================================
-- Configuration
-- ============================================================

-- H3 resolution for spatial indexing
-- GFS 0.25° grid = ~28km at equator, so we pick H3 res 5 (~8.5km edge)
-- which gives ~3 H3 cells per GFS grid point for good interpolation
--
-- | H3 Res | Edge Length | Use Case                    |
-- |--------|-------------|------------------------------|
-- |   3    | ~70 km      | Coarse continental view      |
-- |   4    | ~22 km      | Matches GFS resolution       |
-- |   5    | ~8.5 km     | Recommended: good for queries|
-- |   6    | ~3.2 km     | High-res regional models     |
-- |   7    | ~1.2 km     | Urban/local models           |
--
-- Lower res = smaller files, faster queries, less precision
-- Higher res = larger files, slower queries, more precision
SET VARIABLE h3_res = 5;

-- WebDAV credentials (set via environment variables)
-- export WEBDAV_USER=your_username
-- export WEBDAV_PASS=your_password
CREATE OR REPLACE SECRET hetzner (
    TYPE WEBDAV,
    USERNAME getenv('WEBDAV_USER'),
    PASSWORD getenv('WEBDAV_PASS'),
    SCOPE 'storagebox://' || getenv('WEBDAV_USER')
);

SELECT 'Weather pipeline starting...' as status;
SELECT 'Run: ' || strftime(now() AT TIME ZONE 'UTC', '%Y-%m-%d') || ' 00Z' as info;
SELECT 'Fetching 36 forecast files from NOAA GFS...' as status;

-- ============================================================
-- Process all files and upload to WebDAV
-- Uses noaa_gfs_forecast_api() with filter pushdown
-- ============================================================

COPY (
    SELECT
        h3_latlng_to_cell(latitude, longitude, getvariable('h3_res'))::UBIGINT as h3_index,
        (strptime(run_date, '%Y%m%d') + interval '1 hour' * run_hour)::TIMESTAMPTZ as model_run_at,
        (strptime(run_date, '%Y%m%d') + interval '1 hour' * (run_hour + forecast_hour))::TIMESTAMPTZ as forecast_at,
        -- Temperature: 2m above ground (K → C)
        MAX(CASE WHEN variable = 'temperature' AND level = '2m'
            THEN value - 273.15 END) as temperature_celsius,
        -- Humidity: 2m above ground (%)
        MAX(CASE WHEN variable = 'humidity' AND level = '2m'
            THEN value END) as humidity_percentage,
        -- Wind speed: derived from U,V at 10m (m/s)
        sqrt(
            pow(MAX(CASE WHEN variable = 'wind_u' AND level = '10m' THEN value END), 2) +
            pow(MAX(CASE WHEN variable = 'wind_v' AND level = '10m' THEN value END), 2)
        ) as wind_speed_ms,
        -- Wind direction: meteorological (where wind comes FROM, degrees)
        (270 - degrees(atan2(
            MAX(CASE WHEN variable = 'wind_v' AND level = '10m' THEN value END),
            MAX(CASE WHEN variable = 'wind_u' AND level = '10m' THEN value END)
        )) + 360) % 360 as wind_direction_deg,
        -- Precipitation: surface (kg/m²)
        MAX(CASE WHEN variable = 'precipitation' AND level = 'surface'
            THEN value END) as precipitation_surface_kgm2,
        -- Wind gust: surface (m/s)
        MAX(CASE WHEN variable = 'gust' AND level = 'surface'
            THEN value END) as gust_surface_ms,
        -- Cloud cover: entire atmosphere (%)
        MAX(CASE WHEN variable = 'clouds' AND level = 'atmosphere'
            THEN value END) as cloud_cover_percentage,
        -- Pressure: mean sea level (Pa → hPa)
        MAX(CASE WHEN variable = 'pressure' AND level = 'msl'
            THEN value / 100.0 END) as sea_level_pressure_hpa
    FROM noaa_gfs_forecast_api()
    WHERE run_date = strftime(now() AT TIME ZONE 'UTC', '%Y%m%d')
      AND run_hour = 0
      AND forecast_hour IN (
          -- Next 24h: every 3h (critical for short-term forecasts)
          0, 3, 6, 9, 12, 15, 18, 21, 24,
          -- Days 2-5: every 6h (good resolution)
          30, 36, 42, 48, 54, 60, 66, 72, 78, 84, 90, 96, 102, 108, 114, 120,
          -- Days 6-16: every 24h (daily trend)
          144, 168, 192, 216, 240, 264, 288, 312, 336, 360, 384
      )
      AND variable IN ('temperature', 'humidity', 'wind_u', 'wind_v', 'precipitation', 'gust', 'clouds', 'pressure')
      AND level IN ('2m', '10m', 'surface', 'atmosphere', 'msl')
    GROUP BY h3_index, model_run_at, forecast_at
    ORDER BY h3_index, forecast_at
) TO 'storagebox://${WEBDAV_USER}/weather/global/forecast.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);

SELECT 'Pipeline complete!' as status;

-- ============================================================
-- Regional Pipeline Example (Finland only)
-- ============================================================
-- Uncomment to run a smaller regional pipeline:
/*
COPY (
    SELECT
        h3_latlng_to_cell(latitude, longitude, 5)::UBIGINT as h3_index,
        (strptime(run_date, '%Y%m%d') + interval '1 hour' * run_hour)::TIMESTAMPTZ as model_run_at,
        (strptime(run_date, '%Y%m%d') + interval '1 hour' * (run_hour + forecast_hour))::TIMESTAMPTZ as forecast_at,
        MAX(CASE WHEN variable = 'temperature' AND level = '2m'
            THEN value - 273.15 END) as temperature_celsius,
        MAX(CASE WHEN variable = 'humidity' AND level = '2m'
            THEN value END) as humidity_percentage,
        MAX(CASE WHEN variable = 'precipitation' AND level = 'surface'
            THEN value END) as precipitation_surface_kgm2
    FROM noaa_gfs_forecast_api()
    WHERE run_date = strftime(now() AT TIME ZONE 'UTC', '%Y%m%d')
      AND run_hour = 0
      AND forecast_hour IN (0, 3, 6, 9, 12, 24, 48, 72)
      AND variable IN ('temperature', 'humidity', 'precipitation')
      AND level IN ('2m', 'surface')
      AND latitude BETWEEN 58 AND 70
      AND longitude BETWEEN 20 AND 32
    GROUP BY h3_index, model_run_at, forecast_at
    ORDER BY h3_index, forecast_at
) TO 'finland_weather.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
*/

-- ============================================================
-- Query example (Tampere, Finland)
-- ============================================================

SELECT 'Query Tampere weather with:' as example;
SELECT $$
INSTALL h3 FROM community;
INSTALL webdavfs FROM community;
LOAD h3;
LOAD webdavfs;

CREATE SECRET (TYPE WEBDAV, USERNAME 'your_user', PASSWORD 'your_pass', SCOPE 'storagebox://your_user');

WITH target AS (SELECT h3_latlng_to_cell(61.5, 23.8, 5)::UBIGINT as h3_idx)
SELECT
    forecast_at,
    round(temperature_celsius, 1) as temp_c,
    round(humidity_percentage, 0) as humidity_pct,
    round(wind_speed_ms, 1) as wind_ms,
    round(wind_direction_deg, 0) as wind_dir,
    round(cloud_cover_percentage, 0) as clouds_pct,
    round(sea_level_pressure_hpa, 1) as pressure_hpa
FROM read_parquet('storagebox://your_user/weather/global/forecast.parquet'), target
WHERE h3_index IN (SELECT unnest(h3_grid_disk(h3_idx, 1)) FROM target)
ORDER BY forecast_at;
$$ as query;
