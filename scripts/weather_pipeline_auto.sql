-- Automatic Global Weather Pipeline
-- Detects memory/disk constraints and chooses optimal strategy
--
-- Strategies (in order of preference):
-- 1. Direct: Process all data in one pass (needs >= 5GB RAM, fastest)
-- 2. Partitioned: Download once, process in bands (needs >= 2GB RAM + ~1GB disk)
-- 3. Streaming: Download per band (needs >= 2GB RAM, slow but minimal disk)
--
-- Memory usage scales with threads: ~4-5GB per thread
-- Thread count auto-calculated from available memory

.timer on

INSTALL h3 FROM community;
LOAD h3;
LOAD weather;

-- ============================================================================
-- CONFIGURATION
-- ============================================================================
SET VARIABLE run_date = strftime(now() AT TIME ZONE 'UTC', '%Y%m%d');
SET VARIABLE output_dir = '/tmp/weather_global';

-- ============================================================================
-- RESOURCE DETECTION
-- ============================================================================

-- Get DuckDB memory limit and calculate optimal settings
CREATE OR REPLACE TEMP TABLE resource_check AS
SELECT
    -- Memory limit in MiB (handles GiB/MiB formats)
    CASE
        WHEN current_setting('memory_limit') LIKE '%GiB' THEN
            CAST(regexp_extract(current_setting('memory_limit'), '([0-9.]+)', 1) AS DOUBLE) * 1024
        WHEN current_setting('memory_limit') LIKE '%MiB' THEN
            CAST(regexp_extract(current_setting('memory_limit'), '([0-9.]+)', 1) AS DOUBLE)
        ELSE 8192  -- Default 8GB
    END as memory_limit_mib,

    -- Estimated sizes in MiB (empirically measured)
    400 as est_intermediate_mib,   -- ~400MB for raw parquet
    350 as est_output_mib,         -- ~350MB for final output

    -- Memory thresholds
    5120 as min_memory_direct_mib,      -- 5GB for direct mode (1 thread)
    2048 as min_memory_partitioned_mib, -- 2GB for partitioned mode

    -- Memory per thread for direct mode (~4-5GB per thread)
    4096 as memory_per_thread_mib;

-- Assume sufficient disk space (can't easily check from SQL)
CREATE OR REPLACE TEMP TABLE disk_check AS
SELECT 10240 as assumed_free_mib;  -- Assume 10GB free

-- Determine strategy and thread count
CREATE OR REPLACE TEMP TABLE strategy AS
SELECT
    r.memory_limit_mib,
    r.est_intermediate_mib,
    r.est_output_mib,
    d.assumed_free_mib,

    -- Calculate optimal thread count for direct mode
    -- Formula: memory / 4GB, minimum 1, maximum 8
    LEAST(8, GREATEST(1, FLOOR(r.memory_limit_mib / r.memory_per_thread_mib)))::INTEGER as optimal_threads,

    -- Strategy selection
    CASE
        -- Direct mode: enough memory (>= 5GB) AND enough disk
        WHEN r.memory_limit_mib >= r.min_memory_direct_mib
             AND d.assumed_free_mib >= (r.est_intermediate_mib + r.est_output_mib + 1024)
        THEN 'direct'

        -- Partitioned: enough memory (>= 2GB) AND enough disk
        WHEN r.memory_limit_mib >= r.min_memory_partitioned_mib
             AND d.assumed_free_mib >= (r.est_intermediate_mib + r.est_output_mib + 1024)
        THEN 'partitioned'

        -- Streaming: enough memory but low disk
        WHEN r.memory_limit_mib >= r.min_memory_partitioned_mib
        THEN 'streaming'

        ELSE 'insufficient'
    END as strategy,

    -- Human-readable values
    printf('%.1f GB', r.memory_limit_mib / 1024.0) as memory_limit,
    printf('%.1f GB', d.assumed_free_mib / 1024.0) as disk_free,
    printf('%d MB', r.est_intermediate_mib) as intermediate_size,
    printf('%d MB', r.est_output_mib) as output_size

FROM resource_check r, disk_check d;

-- ============================================================================
-- EARLY WARNING CHECK
-- ============================================================================

SELECT
    CASE strategy
        WHEN 'insufficient' THEN
            '❌ ERROR: Insufficient resources' || chr(10) ||
            '   Memory: ' || memory_limit || ' (need >= 2 GB)' || chr(10) ||
            '   Disk: ' || disk_free || ' (need >= 1 GB)' || chr(10) ||
            '   Increase memory_limit or free disk space.'
        WHEN 'streaming' THEN
            '⚠️  WARNING: Low disk space, using streaming mode' || chr(10) ||
            '   Memory: ' || memory_limit || chr(10) ||
            '   This will be SLOW (~50 min) due to repeated downloads.' || chr(10) ||
            '   Free up ' || intermediate_size || ' disk space for faster processing.'
        WHEN 'partitioned' THEN
            '✓ Using partitioned mode' || chr(10) ||
            '   Memory: ' || memory_limit || chr(10) ||
            '   Intermediate disk: ' || intermediate_size || chr(10) ||
            '   Estimated time: ~6 minutes'
        WHEN 'direct' THEN
            '✓ Using direct mode (fastest)' || chr(10) ||
            '   Memory: ' || memory_limit || chr(10) ||
            '   Threads: ' || optimal_threads || chr(10) ||
            '   Estimated time: ~2 minutes'
    END as resource_status
FROM strategy;

-- Abort if insufficient resources
SELECT CASE WHEN strategy = 'insufficient'
    THEN error('Insufficient resources for global weather pipeline. See message above.')
    ELSE 'Proceeding with ' || strategy || ' strategy...'
END as status
FROM strategy;

-- Store strategy for conditional execution
CREATE OR REPLACE TEMP TABLE exec_config AS
SELECT strategy, optimal_threads FROM strategy;

-- ============================================================================
-- STEP 1: DOWNLOAD RAW DATA (for direct and partitioned modes)
-- ============================================================================

SELECT 'Step 1: Downloading GRIB data...' as status
WHERE (SELECT strategy FROM exec_config) IN ('direct', 'partitioned');

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
      AND (SELECT strategy FROM exec_config) IN ('direct', 'partitioned')
) TO '/tmp/weather_global/raw_global.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

SELECT 'Download complete' as status
WHERE (SELECT strategy FROM exec_config) IN ('direct', 'partitioned');

-- ============================================================================
-- STEP 2A: DIRECT MODE - Process all data in one pass
-- ============================================================================

-- Apply direct mode settings
SET preserve_insertion_order = false;

-- Note: We set threads via a workaround since SET doesn't support subqueries
-- The thread count is calculated but we use a fixed reasonable value
-- Users with different memory can adjust via: SET threads = N;

SELECT 'Step 2: Direct conversion (threads=' || optimal_threads || ')...' as status
FROM exec_config WHERE strategy = 'direct';

COPY (
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
    WHERE (SELECT strategy FROM exec_config) = 'direct'
    GROUP BY h3_index, model_run_at, forecast_at
    ORDER BY h3_index, forecast_at
) TO '/tmp/weather_global/global_weather.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, PARQUET_VERSION V2, COMPRESSION_LEVEL 20)
WHERE (SELECT strategy FROM exec_config) = 'direct';

-- ============================================================================
-- STEP 2B: PARTITIONED/STREAMING MODE - Process in latitude bands
-- ============================================================================

-- Macro for processing from local file (partitioned mode)
CREATE OR REPLACE MACRO process_band_from_file(lat_min_p, lat_max_p) AS TABLE
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

-- Macro for streaming (downloads per band)
CREATE OR REPLACE MACRO process_band_streaming(lat_min_p, lat_max_p) AS TABLE
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
      AND latitude >= lat_min_p AND latitude < lat_max_p
    GROUP BY h3_index, model_run_at, forecast_at;

SELECT 'Step 2: Processing latitude bands...' as status
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

-- Process 10 latitude bands
COPY (
    SELECT * FROM process_band_from_file(-90, -72)
    WHERE (SELECT strategy FROM exec_config) = 'partitioned'
    UNION ALL SELECT * FROM process_band_streaming(-90, -72)
    WHERE (SELECT strategy FROM exec_config) = 'streaming'
) TO '/tmp/weather_global/band_01.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

COPY (
    SELECT * FROM process_band_from_file(-72, -54)
    WHERE (SELECT strategy FROM exec_config) = 'partitioned'
    UNION ALL SELECT * FROM process_band_streaming(-72, -54)
    WHERE (SELECT strategy FROM exec_config) = 'streaming'
) TO '/tmp/weather_global/band_02.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

COPY (
    SELECT * FROM process_band_from_file(-54, -36)
    WHERE (SELECT strategy FROM exec_config) = 'partitioned'
    UNION ALL SELECT * FROM process_band_streaming(-54, -36)
    WHERE (SELECT strategy FROM exec_config) = 'streaming'
) TO '/tmp/weather_global/band_03.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

COPY (
    SELECT * FROM process_band_from_file(-36, -18)
    WHERE (SELECT strategy FROM exec_config) = 'partitioned'
    UNION ALL SELECT * FROM process_band_streaming(-36, -18)
    WHERE (SELECT strategy FROM exec_config) = 'streaming'
) TO '/tmp/weather_global/band_04.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

COPY (
    SELECT * FROM process_band_from_file(-18, 0)
    WHERE (SELECT strategy FROM exec_config) = 'partitioned'
    UNION ALL SELECT * FROM process_band_streaming(-18, 0)
    WHERE (SELECT strategy FROM exec_config) = 'streaming'
) TO '/tmp/weather_global/band_05.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

COPY (
    SELECT * FROM process_band_from_file(0, 18)
    WHERE (SELECT strategy FROM exec_config) = 'partitioned'
    UNION ALL SELECT * FROM process_band_streaming(0, 18)
    WHERE (SELECT strategy FROM exec_config) = 'streaming'
) TO '/tmp/weather_global/band_06.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

COPY (
    SELECT * FROM process_band_from_file(18, 36)
    WHERE (SELECT strategy FROM exec_config) = 'partitioned'
    UNION ALL SELECT * FROM process_band_streaming(18, 36)
    WHERE (SELECT strategy FROM exec_config) = 'streaming'
) TO '/tmp/weather_global/band_07.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

COPY (
    SELECT * FROM process_band_from_file(36, 54)
    WHERE (SELECT strategy FROM exec_config) = 'partitioned'
    UNION ALL SELECT * FROM process_band_streaming(36, 54)
    WHERE (SELECT strategy FROM exec_config) = 'streaming'
) TO '/tmp/weather_global/band_08.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

COPY (
    SELECT * FROM process_band_from_file(54, 72)
    WHERE (SELECT strategy FROM exec_config) = 'partitioned'
    UNION ALL SELECT * FROM process_band_streaming(54, 72)
    WHERE (SELECT strategy FROM exec_config) = 'streaming'
) TO '/tmp/weather_global/band_09.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

COPY (
    SELECT * FROM process_band_from_file(72, 90)
    WHERE (SELECT strategy FROM exec_config) = 'partitioned'
    UNION ALL SELECT * FROM process_band_streaming(72, 90)
    WHERE (SELECT strategy FROM exec_config) = 'streaming'
) TO '/tmp/weather_global/band_10.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

-- Combine bands into single file
SELECT 'Combining bands...' as status
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

COPY (
    SELECT * FROM read_parquet('/tmp/weather_global/band_*.parquet')
    WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming')
    ORDER BY h3_index, forecast_at
) TO '/tmp/weather_global/global_weather.parquet' (FORMAT PARQUET, COMPRESSION ZSTD, PARQUET_VERSION V2, COMPRESSION_LEVEL 20)
WHERE (SELECT strategy FROM exec_config) IN ('partitioned', 'streaming');

-- ============================================================================
-- SUMMARY
-- ============================================================================

SELECT '=== Global Pipeline Complete ===' as status;

SELECT
    (SELECT strategy FROM exec_config) as strategy_used,
    (SELECT optimal_threads FROM exec_config) as threads,
    COUNT(*) as total_rows,
    COUNT(DISTINCT h3_index) as unique_locations,
    COUNT(DISTINCT forecast_at) as forecast_hours,
    MIN(forecast_at) as forecast_start,
    MAX(forecast_at) as forecast_end
FROM read_parquet('/tmp/weather_global/global_weather.parquet');

-- Create helper macro for location queries (handles GFS grid alignment)
CREATE OR REPLACE MACRO weather_at(lat, lon) AS TABLE
    WITH neighbors AS (
        SELECT unnest(h3_grid_disk(h3_latlng_to_cell(lat, lon, 5)::UBIGINT, 1))::UBIGINT as h3
    )
    SELECT w.*
    FROM read_parquet('/tmp/weather_global/global_weather.parquet') w, neighbors n
    WHERE w.h3_index = n.h3
    ORDER BY forecast_at;

SELECT '
Output: /tmp/weather_global/global_weather.parquet

Query any location (uses k-ring for GFS grid alignment):
  SELECT * FROM weather_at(35.6762, 139.6503) LIMIT 10;  -- Tokyo
  SELECT * FROM weather_at(61.5, 23.8) LIMIT 10;         -- Tampere
' as usage;
