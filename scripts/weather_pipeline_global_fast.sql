-- Global Weather Pipeline (Fast version)
-- Downloads GRIB files once, then processes by latitude bands
-- ~6 minutes total instead of ~50 minutes
--
-- Design principle: Prefer disk over HTTP
-- - Intermediate parquet (~400 MB) is cheaper than 10x HTTP round-trips
-- - Each GRIB download has latency; local parquet reads are instant

.timer on

INSTALL h3 FROM community;
LOAD h3;
LOAD weather;

SET VARIABLE run_date = strftime(now() AT TIME ZONE 'UTC', '%Y%m%d');

SELECT 'Step 1: Download all GRIB files once...' as status;

-- Download all forecast hours to a single intermediate parquet
-- This takes ~5 minutes but only downloads once
COPY (
    SELECT
        latitude, longitude, value, variable, level,
        run_date, run_hour, forecast_hour
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
) TO '/tmp/weather_global/raw_global.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Report raw data size (using glob to get file info)
SELECT 'Raw data downloaded' as status;

SELECT 'Step 2: Process latitude bands from downloaded data...' as status;

-- Now process each band from local parquet (very fast, no HTTP)
CREATE OR REPLACE MACRO process_band_local(lat_min_p, lat_max_p) AS TABLE
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
    FROM read_parquet('/tmp/weather_global/raw_global.parquet')
    WHERE latitude >= lat_min_p AND latitude < lat_max_p
    GROUP BY h3_index, model_run_at, forecast_at;

SELECT '  Band 1/10: South Polar' as status;
COPY (FROM process_band_local(-90, -72))
    TO '/tmp/weather_global/band_01.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '  Band 2/10: Southern' as status;
COPY (FROM process_band_local(-72, -54))
    TO '/tmp/weather_global/band_02.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '  Band 3/10: South Temperate' as status;
COPY (FROM process_band_local(-54, -36))
    TO '/tmp/weather_global/band_03.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '  Band 4/10: South Subtropical' as status;
COPY (FROM process_band_local(-36, -18))
    TO '/tmp/weather_global/band_04.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '  Band 5/10: South Tropical' as status;
COPY (FROM process_band_local(-18, 0))
    TO '/tmp/weather_global/band_05.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '  Band 6/10: North Tropical' as status;
COPY (FROM process_band_local(0, 18))
    TO '/tmp/weather_global/band_06.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '  Band 7/10: North Subtropical' as status;
COPY (FROM process_band_local(18, 36))
    TO '/tmp/weather_global/band_07.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '  Band 8/10: North Temperate' as status;
COPY (FROM process_band_local(36, 54))
    TO '/tmp/weather_global/band_08.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '  Band 9/10: Northern' as status;
COPY (FROM process_band_local(54, 72))
    TO '/tmp/weather_global/band_09.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT '  Band 10/10: North Polar' as status;
COPY (FROM process_band_local(72, 90))
    TO '/tmp/weather_global/band_10.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

-- Combine into single file with high compression
SELECT 'Combining bands into global_weather.parquet...' as status;

COPY (
    SELECT * FROM read_parquet('/tmp/weather_global/band_*.parquet')
    ORDER BY h3_index, forecast_at
) TO '/tmp/weather_global/global_weather.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, PARQUET_VERSION V2, COMPRESSION_LEVEL 20);

-- Summary
SELECT '=== Global Pipeline Complete ===' as status;

SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT h3_index) as unique_locations,
    COUNT(DISTINCT forecast_at) as forecast_hours,
    MIN(forecast_at) as forecast_start,
    MAX(forecast_at) as forecast_end
FROM read_parquet('/tmp/weather_global/global_weather.parquet');

-- Usage example
SELECT '
Output: /tmp/weather_global/global_weather.parquet

Query any location:
  WITH target AS (SELECT h3_latlng_to_cell(lat, lon, 5)::UBIGINT as h3)
  SELECT * FROM read_parquet(''/tmp/weather_global/global_weather.parquet''), target
  WHERE h3_index = target.h3;
' as usage;
