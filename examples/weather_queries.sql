-- DuckDB Weather Extension - Example Queries
--
-- Prerequisites:
--   1. Load the extension
--   2. Have weather data in Parquet format (see grib_to_parquet.py)
--   3. Optionally install h3 extension for spatial queries

-- Load the weather extension
LOAD 'duckdb_weather.duckdb_extension';

-- Optional: Install H3 extension for spatial indexing
-- INSTALL h3 FROM community;
-- LOAD h3;

-- ============================================================================
-- Basic Weather Queries
-- ============================================================================

-- Query current conditions with unit conversions
SELECT
    h3_index,
    latitude,
    longitude,
    kelvin_to_celsius(temperature_k) as temp_celsius,
    celsius_to_fahrenheit(kelvin_to_celsius(temperature_k)) as temp_fahrenheit,
    relative_humidity as humidity_pct,
    wind_speed(wind_u_ms, wind_v_ms) as wind_speed_ms,
    wind_direction(wind_u_ms, wind_v_ms) as wind_direction_deg,
    pa_to_hpa(surface_pressure_pa) as pressure_hpa,
    cloud_category(cloud_cover) as cloud_conditions
FROM read_parquet('weather.parquet')
LIMIT 10;

-- ============================================================================
-- Find Locations by Weather Conditions
-- ============================================================================

-- Find warm and sunny locations (>25°C, <20% cloud cover)
SELECT
    h3_index,
    latitude,
    longitude,
    kelvin_to_celsius(temperature_k) as temp_c,
    cloud_cover * 100 as cloud_pct
FROM read_parquet('weather.parquet')
WHERE
    kelvin_to_celsius(temperature_k) > 25
    AND cloud_cover < 0.2
ORDER BY temp_c DESC
LIMIT 20;

-- Find locations with high wind (>10 m/s)
SELECT
    h3_index,
    latitude,
    longitude,
    wind_speed(wind_u_ms, wind_v_ms) as wind_ms,
    wind_direction(wind_u_ms, wind_v_ms) as wind_dir,
    beaufort_scale(wind_speed(wind_u_ms, wind_v_ms)) as beaufort
FROM read_parquet('weather.parquet')
WHERE wind_speed(wind_u_ms, wind_v_ms) > 10
ORDER BY wind_ms DESC
LIMIT 20;

-- Find locations with precipitation
SELECT
    h3_index,
    latitude,
    longitude,
    precip_rate_to_mm_hr(precip_rate_kg_m2_s) as precip_mm_hr,
    kelvin_to_celsius(temperature_k) as temp_c
FROM read_parquet('weather.parquet')
WHERE precip_rate_kg_m2_s > 0
ORDER BY precip_rate_kg_m2_s DESC
LIMIT 20;

-- ============================================================================
-- Calculate Apparent Temperature (Heat Index / Wind Chill)
-- ============================================================================

-- Apparent temperature considering humidity and wind
SELECT
    h3_index,
    kelvin_to_celsius(temperature_k) as actual_temp_c,
    CASE
        WHEN kelvin_to_celsius(temperature_k) > 26.7 THEN
            heat_index(kelvin_to_celsius(temperature_k), relative_humidity)
        WHEN kelvin_to_celsius(temperature_k) < 10 THEN
            wind_chill(kelvin_to_celsius(temperature_k), wind_speed(wind_u_ms, wind_v_ms))
        ELSE
            kelvin_to_celsius(temperature_k)
    END as feels_like_c,
    relative_humidity as humidity_pct,
    wind_speed(wind_u_ms, wind_v_ms) as wind_ms
FROM read_parquet('weather.parquet')
LIMIT 20;

-- ============================================================================
-- Aggregate Statistics
-- ============================================================================

-- Global temperature statistics
SELECT
    MIN(kelvin_to_celsius(temperature_k)) as min_temp_c,
    AVG(kelvin_to_celsius(temperature_k)) as avg_temp_c,
    MAX(kelvin_to_celsius(temperature_k)) as max_temp_c,
    STDDEV(kelvin_to_celsius(temperature_k)) as stddev_temp_c
FROM read_parquet('weather.parquet');

-- Temperature distribution by latitude bands
SELECT
    FLOOR(latitude / 10) * 10 as lat_band,
    COUNT(*) as n_points,
    AVG(kelvin_to_celsius(temperature_k)) as avg_temp_c,
    AVG(relative_humidity) as avg_humidity,
    AVG(wind_speed(wind_u_ms, wind_v_ms)) as avg_wind_ms
FROM read_parquet('weather.parquet')
GROUP BY FLOOR(latitude / 10) * 10
ORDER BY lat_band;

-- ============================================================================
-- Time Series Analysis (with multiple forecast files)
-- ============================================================================

-- Query multiple forecast hours
-- SELECT
--     forecast_time,
--     h3_index,
--     kelvin_to_celsius(temperature_k) as temp_c
-- FROM read_parquet('weather_*.parquet')
-- WHERE h3_index = '85283473fffffff'  -- Specific location
-- ORDER BY forecast_time;

-- ============================================================================
-- Spatial Queries with H3 (requires H3 extension)
-- ============================================================================

-- Find weather at a specific location using H3
-- SELECT *
-- FROM read_parquet('weather.parquet')
-- WHERE h3_index = h3_latlng_to_cell(60.17, 24.94, 5);  -- Helsinki

-- Find weather in a region using H3 k-ring
-- WITH target AS (
--     SELECT unnest(h3_grid_disk(h3_latlng_to_cell(60.17, 24.94, 5), 2)) as h3_index
-- )
-- SELECT
--     w.h3_index,
--     kelvin_to_celsius(w.temperature_k) as temp_c,
--     wind_speed(w.wind_u_ms, w.wind_v_ms) as wind_ms
-- FROM read_parquet('weather.parquet') w
-- JOIN target t ON w.h3_index = t.h3_index;

-- ============================================================================
-- Export Processed Data
-- ============================================================================

-- Export processed weather data to Parquet
-- COPY (
--     SELECT
--         h3_index,
--         latitude,
--         longitude,
--         forecast_time,
--         kelvin_to_celsius(temperature_k) as temperature_c,
--         relative_humidity,
--         dew_point(kelvin_to_celsius(temperature_k), relative_humidity) as dewpoint_c,
--         wind_speed(wind_u_ms, wind_v_ms) as wind_speed_ms,
--         wind_direction(wind_u_ms, wind_v_ms) as wind_direction_deg,
--         pa_to_hpa(surface_pressure_pa) as pressure_hpa,
--         cloud_cover,
--         precip_rate_to_mm_hr(precip_rate_kg_m2_s) as precip_rate_mm_hr,
--         visibility_m_to_km(visibility_m) as visibility_km
--     FROM read_parquet('weather.parquet')
-- ) TO 'weather_processed.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
