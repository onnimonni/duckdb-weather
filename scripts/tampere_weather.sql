-- ============================================================
-- Tampere Weather Forecast
-- ============================================================
-- Shows weather forecast for Tampere, Finland (61.5°N, 23.8°E)
-- Usage: duckdb -unsigned -f scripts/tampere_weather.sql
-- ============================================================

-- Install required extensions
INSTALL h3 FROM community;
INSTALL httpfs;

LOAD h3;
LOAD httpfs;

-- Load weather extension
LOAD './build/release/extension/weather/weather.duckdb_extension';

-- Tampere coordinates
SET VARIABLE tampere_lat = 61.5;
SET VARIABLE tampere_lon = 23.8;

SELECT 'Fetching weather forecast for Tampere, Finland...' as status;

-- Fetch and display weather forecast
WITH raw_data AS (
    SELECT
        h3_latlng_to_cell(latitude, longitude, 5)::UBIGINT as h3_index,
        (strptime(run_date, '%Y%m%d') + interval '1 hour' * run_hour)::TIMESTAMPTZ as model_run_at,
        (strptime(run_date, '%Y%m%d') + interval '1 hour' * (run_hour + forecast_hour))::TIMESTAMPTZ as forecast_at,
        variable,
        level,
        value
    FROM noaa_gfs_forecast_api()
    WHERE run_date = strftime(now() AT TIME ZONE 'UTC', '%Y%m%d')
      AND run_hour = 0
      AND forecast_hour IN (0, 3, 6, 12, 24, 48, 72)
      AND variable IN ('temperature', 'humidity', 'wind_u', 'wind_v', 'clouds', 'pressure')
      AND level IN ('2m', '10m', 'atmosphere', 'msl')
      AND latitude BETWEEN 61 AND 62
      AND longitude BETWEEN 23 AND 25
),
pivoted AS (
    SELECT
        h3_index,
        model_run_at,
        forecast_at,
        MAX(CASE WHEN variable = 'temperature' AND level = '2m' THEN value - 273.15 END) as temp_c,
        MAX(CASE WHEN variable = 'humidity' AND level = '2m' THEN value END) as humidity_pct,
        MAX(CASE WHEN variable = 'wind_u' AND level = '10m' THEN value END) as wind_u,
        MAX(CASE WHEN variable = 'wind_v' AND level = '10m' THEN value END) as wind_v,
        MAX(CASE WHEN variable = 'clouds' AND level = 'atmosphere' THEN value END) as clouds_pct,
        MAX(CASE WHEN variable = 'pressure' AND level = 'msl' THEN value / 100.0 END) as pressure_hpa
    FROM raw_data
    GROUP BY h3_index, model_run_at, forecast_at
),
target AS (
    SELECT h3_latlng_to_cell(getvariable('tampere_lat'), getvariable('tampere_lon'), 5)::UBIGINT as h3_idx
)
SELECT
    strftime(forecast_at, '%a %d %b %H:%M') as forecast,
    round(avg(temp_c), 1) as "°C",
    round(avg(humidity_pct), 0)::INT as "RH%",
    round(avg(sqrt(wind_u^2 + wind_v^2)), 1) as "Wind m/s",
    round(avg(clouds_pct), 0)::INT as "Cloud%",
    round(avg(pressure_hpa), 0)::INT as "hPa"
FROM pivoted, target
WHERE h3_index IN (SELECT unnest(h3_grid_disk(h3_idx, 1)) FROM target)
GROUP BY forecast_at
ORDER BY forecast_at;
