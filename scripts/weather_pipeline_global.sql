-- Global Weather Pipeline (Partitioned by latitude bands)
-- Processes world in 10 bands to avoid OOM, then combines
-- Memory: ~2GB peak per band
-- Disk: ~50MB per band, ~500MB total

.timer on

INSTALL h3 FROM community;
LOAD h3;
LOAD weather;

-- Configuration
SET VARIABLE h3_res = 5;
SET VARIABLE run_date = strftime(now() AT TIME ZONE 'UTC', '%Y%m%d');
SET VARIABLE output_dir = '/tmp/weather_global';

-- Create output directory structure
SELECT 'Creating output directory...' as status;

-- Forecast hours to fetch (key intervals for 10-day forecast)
-- 3-hourly for first 3 days, 6-hourly for days 4-7, 12-hourly for days 8-10
SET VARIABLE forecast_hours = [0, 3, 6, 9, 12, 15, 18, 21, 24,
                               30, 36, 42, 48, 54, 60, 66, 72,
                               84, 96, 108, 120, 132, 144,
                               168, 192, 216, 240];

-- Variables and levels to fetch
SET VARIABLE variables = ['temperature', 'humidity', 'wind_u', 'wind_v',
                          'precipitation', 'gust', 'clouds', 'pressure'];
SET VARIABLE levels = ['2m', '10m', 'surface', 'atmosphere', 'msl'];

-- Define latitude bands (10 bands of 18° each, like Nordic coverage)
-- This keeps each band similar in memory usage to the Nordic pipeline
CREATE OR REPLACE TABLE lat_bands AS
SELECT * FROM (VALUES
    (-90, -72, 'band_01_south_polar'),
    (-72, -54, 'band_02_southern'),
    (-54, -36, 'band_03_south_temperate'),
    (-36, -18, 'band_04_south_subtropical'),
    (-18,   0, 'band_05_south_tropical'),
    (  0,  18, 'band_06_north_tropical'),
    ( 18,  36, 'band_07_north_subtropical'),
    ( 36,  54, 'band_08_north_temperate'),
    ( 54,  72, 'band_09_northern'),
    ( 72,  90, 'band_10_north_polar')
) AS t(lat_min, lat_max, band_name);

-- Macro to process a single latitude band
-- Note: forecast_hour list is explicit because macros don't evaluate getvariable() in IN clauses
CREATE OR REPLACE MACRO process_band(lat_min, lat_max, band_name) AS TABLE
    SELECT
        h3_latlng_to_cell(latitude, longitude, 5)::UBIGINT as h3_index,
        (strptime(run_date, '%Y%m%d') + interval '1 hour' * run_hour)::TIMESTAMPTZ as model_run_at,
        (strptime(run_date, '%Y%m%d') + interval '1 hour' * (run_hour + forecast_hour))::TIMESTAMPTZ as forecast_at,
        (MAX(CASE WHEN variable = 'temperature' AND level = '2m'
            THEN value - 273.15 END))::REAL as temperature_celsius,
        (MAX(CASE WHEN variable = 'humidity' AND level = '2m'
            THEN value END))::REAL as humidity_percentage,
        (MAX(CASE WHEN variable = 'gust' AND level = 'surface'
            THEN value END))::REAL as wind_gust_ms,
        (sqrt(
            pow(MAX(CASE WHEN variable = 'wind_u' AND level = '10m' THEN value END), 2) +
            pow(MAX(CASE WHEN variable = 'wind_v' AND level = '10m' THEN value END), 2)
        ))::REAL as wind_speed_ms,
        ((270 - degrees(atan2(
            MAX(CASE WHEN variable = 'wind_v' AND level = '10m' THEN value END),
            MAX(CASE WHEN variable = 'wind_u' AND level = '10m' THEN value END)
        )) + 360) % 360)::REAL as wind_direction_deg,
        (MAX(CASE WHEN variable = 'precipitation' AND level = 'surface'
            THEN value END))::REAL as precipitation_kgm2,
        (MAX(CASE WHEN variable = 'clouds' AND level = 'atmosphere'
            THEN value END))::REAL as cloud_cover_percentage,
        (MAX(CASE WHEN variable = 'pressure' AND level = 'msl'
            THEN value / 100.0 END))::REAL as sea_level_pressure_hpa
    FROM noaa_gfs_forecast_api()
    WHERE run_date = getvariable('run_date')
      AND run_hour = 0
      AND forecast_hour IN (0, 3, 6, 9, 12, 15, 18, 21, 24,
                            30, 36, 42, 48, 54, 60, 66, 72,
                            84, 96, 108, 120, 132, 144,
                            168, 192, 216, 240)
      AND variable IN ('temperature', 'humidity', 'wind_u', 'wind_v',
                       'precipitation', 'gust', 'clouds', 'pressure')
      AND level IN ('2m', '10m', 'surface', 'atmosphere', 'msl')
      AND latitude >= lat_min AND latitude < lat_max
    GROUP BY h3_index, model_run_at, forecast_at;

-- Process each band sequentially to parquet files
-- This ensures only one band is in memory at a time

SELECT '=== Processing Band 1/10: South Polar (-90 to -72) ===' as status;
COPY (FROM process_band(-90, -72, 'band_01'))
    TO '/tmp/weather_global/band_01.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '=== Processing Band 2/10: Southern (-72 to -54) ===' as status;
COPY (FROM process_band(-72, -54, 'band_02'))
    TO '/tmp/weather_global/band_02.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '=== Processing Band 3/10: South Temperate (-54 to -36) ===' as status;
COPY (FROM process_band(-54, -36, 'band_03'))
    TO '/tmp/weather_global/band_03.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '=== Processing Band 4/10: South Subtropical (-36 to -18) ===' as status;
COPY (FROM process_band(-36, -18, 'band_04'))
    TO '/tmp/weather_global/band_04.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '=== Processing Band 5/10: South Tropical (-18 to 0) ===' as status;
COPY (FROM process_band(-18, 0, 'band_05'))
    TO '/tmp/weather_global/band_05.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '=== Processing Band 6/10: North Tropical (0 to 18) ===' as status;
COPY (FROM process_band(0, 18, 'band_06'))
    TO '/tmp/weather_global/band_06.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '=== Processing Band 7/10: North Subtropical (18 to 36) ===' as status;
COPY (FROM process_band(18, 36, 'band_07'))
    TO '/tmp/weather_global/band_07.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '=== Processing Band 8/10: North Temperate (36 to 54) ===' as status;
COPY (FROM process_band(36, 54, 'band_08'))
    TO '/tmp/weather_global/band_08.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '=== Processing Band 9/10: Northern (54 to 72) ===' as status;
COPY (FROM process_band(54, 72, 'band_09'))
    TO '/tmp/weather_global/band_09.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '=== Processing Band 10/10: North Polar (72 to 90) ===' as status;
COPY (FROM process_band(72, 90, 'band_10'))
    TO '/tmp/weather_global/band_10.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Summary
SELECT '=== Global Pipeline Complete ===' as status;

SELECT
    COUNT(*) as total_files,
    printf('%.1f MB', SUM(size) / 1024.0 / 1024.0) as total_size
FROM glob('/tmp/weather_global/band_*.parquet')
JOIN (SELECT file, size FROM read_blob('/tmp/weather_global/band_*.parquet')) USING (file);

-- Verify data
SELECT
    'Band files created' as status,
    COUNT(DISTINCT file_path) as num_files,
    COUNT(*) as total_rows,
    COUNT(DISTINCT h3_index) as unique_locations,
    MIN(forecast_at) as forecast_start,
    MAX(forecast_at) as forecast_end
FROM read_parquet('/tmp/weather_global/band_*.parquet', filename=true)
    AS t(h3_index, model_run_at, forecast_at, temperature_celsius, humidity_percentage,
         wind_gust_ms, wind_speed_ms, wind_direction_deg, precipitation_kgm2,
         cloud_cover_percentage, sea_level_pressure_hpa, file_path);

-- Query example: combine all bands
SELECT '
To query global data:
  SELECT * FROM read_parquet(''/tmp/weather_global/band_*.parquet'') LIMIT 10;

To get weather for any location:
  WITH target AS (SELECT h3_latlng_to_cell(lat, lon, 5)::UBIGINT as h3)
  SELECT * FROM read_parquet(''/tmp/weather_global/band_*.parquet''), target
  WHERE h3_index = target.h3;
' as usage_example;
