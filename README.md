# DuckDB Weather Extension

DuckDB extension for weather data: read GRIB2 files, use H3 spatial indexing, and weather utility functions.

## Features

- **`noaa_gfs_forecast_api()`** - Query NOAA GFS forecasts with SQL WHERE filters
- **`read_grib(path)`** - Read GRIB2 files directly as tables
- **Weather macros** - Temperature, wind, pressure, humidity, comfort indices
- **GRIB2 ENUMs** - Human-readable parameter names
- **H3 integration** - Works with DuckDB H3 extension for spatial queries

## Installation

```sql
-- Load from local build
LOAD './build/release/weather.duckdb_extension';

-- Also install H3 for spatial queries
INSTALL h3 FROM community;
LOAD h3;
```

## Quick Start

```sql
LOAD weather;
INSTALL h3 FROM community;
LOAD h3;

-- Read GRIB2 file
SELECT * FROM read_grib('/tmp/gfs.grib2') LIMIT 10;

-- Use weather macros
SELECT
    kelvin_to_celsius(300.0) as temp_c,
    wind_speed(5.0, 5.0) as wind_ms,
    beaufort_description(15.0) as wind_desc;
```

## noaa_gfs_forecast_api() - NOAA GFS API with SQL Filters

Query NOAA GFS weather forecasts directly with SQL WHERE clauses. Filters are pushed down to the API for efficient data fetching.

```sql
-- Get temperature forecast for Tampere, Finland
SELECT variable, level, round(kelvin_to_celsius(AVG(value)), 1) as temp_c
FROM noaa_gfs_forecast_api()
WHERE run_date = current_date
  AND run_hour = 0
  AND forecast_hour = 24
  AND variable = 'temperature'
  AND level = '2m'
  AND latitude >= 61 AND latitude <= 62
  AND longitude >= 23 AND longitude <= 24
GROUP BY variable, level;

-- Multiple variables and levels with BETWEEN
SELECT variable, level, round(AVG(value), 1) as avg_val
FROM noaa_gfs_forecast_api()
WHERE forecast_hour IN (0, 6, 12, 24)
  AND variable IN ('temperature', 'humidity', 'wind_u', 'wind_v')
  AND level IN ('2m', '10m')
  AND latitude BETWEEN 58 AND 65
  AND longitude BETWEEN 20 AND 28
GROUP BY variable, level;

-- Global data (no lat/lon filter = worldwide)
SELECT variable, COUNT(*) as points
FROM noaa_gfs_forecast_api()
WHERE forecast_hour = 0
  AND variable = 'temperature'
  AND level = '2m'
GROUP BY variable;
```

**Supported WHERE filters (pushed to API):**

| Filter | Example | Maps to |
| ------ | ------- | ------- |
| `run_date` | `= '2026-01-20'` or `= current_date` | `dir=/gfs.YYYYMMDD/...` |
| `run_hour` | `= 0` (0, 6, 12, 18) | `dir=.../HH/atmos` |
| `forecast_hour` | `= 24` or `IN (0, 6, 12)` | `file=...fFFF` |
| `variable` | `= 'temperature'` or `IN (...)` | `var_TMP=on` etc. |
| `level` | `= '2m'` or `IN ('2m', '10m')` | `lev_2_m_above_ground=on` |
| `latitude` | `BETWEEN 58 AND 65` | `toplat=65&bottomlat=58` |
| `longitude` | `BETWEEN 20 AND 28` | `leftlon=20&rightlon=28` |
| (no lat/lon) | (omit filters) | global data (full grid) |

**Variable aliases:**

| Human name | API parameter |
| ---------- | ------------- |
| `temperature`, `temp`, `t` | `var_TMP` |
| `humidity`, `relative_humidity`, `rh` | `var_RH` |
| `wind_u`, `u_wind`, `ugrd` | `var_UGRD` |
| `wind_v`, `v_wind`, `vgrd` | `var_VGRD` |
| `precipitation`, `precip`, `rain` | `var_APCP` |
| `gust`, `wind_gust` | `var_GUST` |
| `clouds`, `cloud_cover` | `var_TCDC` |
| `pressure`, `msl_pressure` | `var_PRMSL` |

**Level aliases:**

| Human name | API parameter |
| ---------- | ------------- |
| `2m`, `2_m`, `2m_above_ground` | `lev_2_m_above_ground` |
| `10m`, `10_m`, `10m_above_ground` | `lev_10_m_above_ground` |
| `surface`, `sfc` | `lev_surface` |
| `atmosphere`, `entire_atmosphere` | `lev_entire_atmosphere` |
| `msl`, `mean_sea_level` | `lev_mean_sea_level` |

## read_grib() Function

Read GRIB2 weather files directly from local files or HTTP URLs. Supports single path or array of paths.

```sql
-- Local file
SELECT * FROM read_grib('/tmp/gfs.grib2') LIMIT 10;

-- Direct HTTP from NOAA (no download needed!)
SELECT
    latitude, longitude,
    kelvin_to_celsius(value) as temp_c
FROM read_grib('https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?'
    || 'dir=%2Fgfs.20260120%2F00%2Fatmos'
    || '&file=gfs.t00z.pgrb2.0p25.f000'
    || '&var_TMP=on&lev_2_m_above_ground=on'
    || '&subregion=&toplat=65&leftlon=20&rightlon=28&bottomlat=58')
WHERE parameter = 'Temperature';

-- Array of URLs (processes all files, adds file_index column)
SELECT
    file_index,
    COUNT(*) as points,
    round(AVG(kelvin_to_celsius(value)), 1) as avg_temp_c
FROM read_grib(['https://url1.grib2', 'https://url2.grib2'])
GROUP BY file_index;
```

**Output columns:**

| Column | Type | Description |
| ------ | ---- | ----------- |
| latitude | DOUBLE | Latitude (-90 to 90) |
| longitude | DOUBLE | Longitude (-180 to 180) |
| value | DOUBLE | Raw value (K, m/s, Pa, %) |
| discipline | ENUM | Meteorological, Hydrological, etc. |
| surface | ENUM | Height_Above_Ground, Isobaric, etc. |
| parameter | ENUM | Temperature, Wind_Speed, etc. |
| forecast_time | BIGINT | Forecast hours from model run |
| surface_value | DOUBLE | Level value (2m, 500hPa, etc.) |
| message_index | UINT32 | GRIB message identifier |
| file_index | UINT32 | Index of source file (0-based, for arrays) |

## read_grib_lateral() - LATERAL Join Support

Use `read_grib_lateral()` when the path comes from another table or CTE (LATERAL join context).

```sql
-- Define URL macro
SET VARIABLE run_date = strftime(now() AT TIME ZONE 'UTC', '%Y%m%d');
CREATE MACRO gfs_url(fhour) AS
    'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?' ||
    'dir=%2Fgfs.' || getvariable('run_date') || '%2F00%2Fatmos' ||
    '&file=gfs.t00z.pgrb2.0p25.f' || printf('%03d', fhour) ||
    '&var_TMP=on&lev_2_m_above_ground=on' ||
    '&subregion=&toplat=62&leftlon=23&rightlon=24&bottomlat=61';

-- LATERAL join: read multiple forecast hours dynamically
WITH fhours AS (SELECT unnest([0, 6, 12, 24]) as fhour)
SELECT
    f.fhour,
    round(AVG(kelvin_to_celsius(g.value)), 1) as avg_temp_c
FROM fhours f, LATERAL read_grib_lateral(gfs_url(f.fhour)) g
GROUP BY f.fhour
ORDER BY f.fhour;
```

## Weather Macros

### Temperature

```sql
SELECT kelvin_to_celsius(300.0);           -- 26.85
SELECT celsius_to_fahrenheit(26.85);       -- 80.33
SELECT fahrenheit_to_celsius(80.0);        -- 26.67
SELECT kelvin_to_fahrenheit(300.0);        -- 80.33
```

### Wind

```sql
SELECT wind_speed(u, v);                   -- Speed from U,V components (m/s)
SELECT wind_direction(u, v);               -- Direction in degrees
SELECT cardinal_direction(225.0);          -- 'SW'
SELECT wind_speed_kmh(10.0);               -- 36 km/h
SELECT wind_speed_mph(10.0);               -- 22.4 mph
SELECT wind_speed_knots(10.0);             -- 19.4 knots
SELECT beaufort_scale(15.0);               -- 7
SELECT beaufort_description(15.0);         -- 'High wind'
```

### Pressure

```sql
SELECT pa_to_hpa(101325);                  -- 1013.25 hPa
SELECT hpa_to_inhg(1013.25);               -- 29.92 inHg
```

### Humidity & Comfort

```sql
SELECT dew_point(25.0, 60.0);              -- 16.7°C
SELECT heat_index(35.0, 80.0);             -- 51.6°C (hot & humid)
SELECT wind_chill(-10.0, 20.0);            -- -17.9°C (cold & windy)
SELECT feels_like(-5.0, 80.0, 25.0);       -- -12.3°C (apparent temp)
```

### Descriptions

```sql
SELECT visibility_category(500);           -- 'Fog'
SELECT precip_intensity(5.0);              -- 'Moderate'
SELECT cloud_description(75.0);            -- 'Mostly cloudy'
SELECT uv_category(8.0);                   -- 'Very high'
```

### Unit Conversion

```sql
SELECT meters_to_km(5000);                 -- 5.0
SELECT meters_to_miles(1609);              -- ~1.0
SELECT meters_to_feet(100);                -- 328.1
SELECT mm_to_inches(25.4);                 -- 1.0
```

## H3 Spatial Queries

Convert GRIB data to H3-indexed parquet for efficient spatial queries.

### Create H3-indexed parquet

```sql
INSTALL h3 FROM community;
LOAD h3;
LOAD weather;

COPY (
    SELECT
        h3_latlng_to_cell(latitude, longitude, 5)::UBIGINT as h3_index,
        latitude, longitude,
        kelvin_to_celsius(value) as temp_c,
        parameter::VARCHAR as parameter,
        forecast_time
    FROM read_grib('/tmp/gfs.grib2')
    WHERE parameter = 'Temperature'
    ORDER BY h3_index
) TO '/tmp/weather_h3.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);
```

### Query by coordinates

```sql
-- Get H3 cell for coordinates
WITH target AS (
    SELECT h3_latlng_to_cell(61.5, 23.8, 5)::UBIGINT as h3_idx
)
SELECT
    h3_cell_to_lat(h3_index)::DECIMAL(6,3) as lat,
    h3_cell_to_lng(h3_index)::DECIMAL(6,3) as lng,
    temp_c,
    forecast_time as hours
FROM read_parquet('/tmp/weather_h3.parquet'), target
WHERE h3_index = target.h3_idx
ORDER BY forecast_time;
```

### H3 Resolution Guide

| Resolution | Edge | Use Case |
| ---------- | ---- | -------- |
| 4 | ~22 km | Global models (GFS) |
| **5** | **~8.5 km** | Recommended for GFS/ECMWF |
| 6 | ~3.2 km | Regional models (HRRR) |
| 7 | ~1.2 km | High-res/urban |

## Remote Storage (WebDAV)

Store and query weather data from Hetzner Storage Box.

### Upload

```bash
# Using curl
curl -T weather.parquet \
  https://your_user.your-storagebox.de/weather/data.parquet \
  -u "your_user:password"
```

### Query remotely

```sql
INSTALL webdavfs FROM community;
LOAD webdavfs;

CREATE SECRET hetzner (
    TYPE WEBDAV,
    USERNAME 'your_user',
    PASSWORD 'your_password',
    SCOPE 'storagebox://your_user'
);

SELECT * FROM read_parquet('storagebox://your_user/weather/*.parquet');
```

## Weather Pipeline

Run the complete NOAA GFS -> H3 Parquet -> WebDAV pipeline:

```bash
# Fetch global weather, convert to H3-indexed parquet, upload to WebDAV
duckdb -unsigned -f scripts/weather_pipeline.sql
```

The pipeline:

1. Fetches forecast hours directly from NOAA via HTTP
2. Converts to H3-indexed parquet (resolution 5 = ~8.5km)
3. Combines into single file with high compression

### Global Pipeline

For global data, use the auto-detecting pipeline:

```bash
duckdb -unsigned -f scripts/weather_pipeline_auto.sql
```

This automatically chooses the optimal strategy based on available resources:

| Memory | Disk | Strategy | Time |
| ------ | ---- | -------- | ---- |
| < 2 GB | any | Error (insufficient) | - |
| 2-5 GB | < 1 GB | Streaming (download per band) | ~50 min |
| 2-5 GB | >= 1 GB | Partitioned (download once, bands) | ~6 min |
| >= 5 GB | >= 1 GB | Direct (single pass) | ~2 min |

### Memory Usage by Thread Count

Direct mode memory scales linearly with threads (~4-5 GB per thread):

| Threads | Peak Memory | Processing Time |
| ------- | ----------- | --------------- |
| 1 | ~5 GB | 160 sec |
| 2 | ~10 GB | 86 sec |
| 4 | ~16 GB | 86 sec |

The pipeline auto-calculates optimal thread count: `memory / 4GB` (max 8).

To measure peak memory usage yourself:

```bash
/usr/bin/time -l duckdb -unsigned -f scripts/weather_pipeline_auto.sql 2>&1 | grep "maximum resident"
```

### Design Principle: Prefer Disk Over HTTP

The pipeline uses intermediate parquet files rather than re-downloading data:

- Intermediate parquet (~400 MB) is cheaper than 10x HTTP round-trips
- Each GRIB download has network latency; local parquet reads are instant
- Trade ~1 GB disk space for 10x faster processing

## Data Sources

### NOAA GFS (Recommended)

| Property | Value |
| -------- | ----- |
| Coverage | Global |
| Resolution | 0.25° (~28km) |
| Updates | 4x daily (00, 06, 12, 18 UTC) |
| Forecast | Up to 384 hours (16 days) |
| License | **Public Domain** |

```bash
# Download Finland region
curl -o gfs.grib2 \
  "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?\
dir=%2Fgfs.$(date -u +%Y%m%d)%2F00%2Fatmos&\
file=gfs.t00z.pgrb2.0p25.f024&\
var_TMP=on&var_RH=on&lev_2_m_above_ground=on&\
subregion=&toplat=65&leftlon=20&rightlon=28&bottomlat=58"
```

See GitHub issues for ECMWF, HRRR, and other sources.

## Example: 16-Day Forecast

```sql
LOAD weather;

-- Read all forecast files
WITH forecast AS (
    SELECT
        forecast_time as hours,
        parameter,
        value
    FROM read_grib('/tmp/gfs_tampere/*.grib2')
    WHERE abs(latitude - 61.5) < 0.25
      AND abs(longitude - 23.8) < 0.25
)
SELECT
    hours,
    round(kelvin_to_celsius(max(CASE WHEN parameter = 'Temperature' THEN value END)), 1) as temp_c,
    round(max(CASE WHEN parameter = 'Relative_Humidity' THEN value END), 0) as rh_pct,
    round(max(CASE WHEN parameter = 'Cloud_Cover' THEN value END), 0) as clouds_pct
FROM forecast
GROUP BY hours
ORDER BY hours;
```

## Building

```bash
git clone --recurse-submodules https://github.com/onnimonni/duckdb-weather
cd duckdb-weather
make release

# Test
./build/release/duckdb -unsigned -c "
LOAD './build/release/repository/v1.4.3/osx_arm64/weather.duckdb_extension';
SELECT kelvin_to_celsius(300.0), beaufort_description(15.0);
"
```

## GRIB2 Parameter Reference

| Discipline | Cat | Num | Parameter |
| ---------- | --- | --- | --------- |
| 0 | 0 | 0 | Temperature (K) |
| 0 | 1 | 1 | Relative Humidity (%) |
| 0 | 1 | 8 | Total Precipitation |
| 0 | 2 | 2 | U-Wind (m/s) |
| 0 | 2 | 3 | V-Wind (m/s) |
| 0 | 2 | 22 | Wind Gust (m/s) |
| 0 | 3 | 1 | Pressure MSL (Pa) |
| 0 | 6 | 1 | Cloud Cover (%) |

Full reference: [NCEP GRIB2 Tables](https://www.nco.ncep.noaa.gov/pmb/docs/grib2/grib2_doc/)

## License

- Extension: MIT
- NOAA data: Public Domain
- ECMWF data: CC-BY-4.0

## Links

- [DuckDB H3 Extension](https://duckdb.org/community_extensions/extensions/h3)
- [NOAA GFS on AWS](https://registry.opendata.aws/noaa-gfs-bdp-pds/)
- [H3 Spatial Indexing](https://h3geo.org/)
