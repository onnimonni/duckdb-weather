# 16-Day Weather Forecast for Tampere

Guide to fetching GFS forecast data and querying with DuckDB.

## GFS Data Structure

**Model runs:** 4x daily at 00z, 06z, 12z, 18z UTC
**Forecast range:** 0-384 hours (16 days)
**Resolution:** 0.25° (~25km)
**Tampere coordinates:** 61.5°N, 23.8°E

## Available Weather Variables

| Variable | GFS Name | Unit | Description |
| -------- | -------- | ---- | ----------- |
| Temperature | TMP | K | Convert: °C = K - 273.15 |
| Precipitation | APCP | kg/m² | Accumulated (same as mm) |
| Relative Humidity | RH | % | |
| Wind U-component | UGRD | m/s | East-west wind |
| Wind V-component | VGRD | m/s | North-south wind |
| Wind Gust | GUST | m/s | Surface gusts |
| Cloud Cover | TCDC | % | Total cloud cover |
| Pressure | PRMSL | Pa | Mean sea level pressure |
| Snow Depth | SNOD | m | |
| Precipitation Rate | PRATE | kg/m²/s | Instantaneous |

## Surface Levels

| Level | Description |
| ----- | ----------- |
| `2_m_above_ground` | Standard weather station height (temp, humidity) |
| `10_m_above_ground` | Wind measurements |
| `surface` | Precipitation, snow, pressure |
| `entire_atmosphere` | Cloud cover |

## Download Commands

### Single variable (small file)

```bash
# 2m Temperature, 24h forecast
curl -o /tmp/gfs_t2m_f024.grib2 \
  "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?\
dir=%2Fgfs.$(date -u +%Y%m%d)%2F00%2Fatmos&\
file=gfs.t00z.pgrb2.0p25.f024&\
var_TMP=on&\
lev_2_m_above_ground=on&\
subregion=&toplat=65&leftlon=20&rightlon=28&bottomlat=58"
```

### Multiple variables for full forecast

```bash
DATE=$(date -u +%Y%m%d)
RUN=00

# Download all forecast hours for Tampere region
for HOUR in 000 003 006 012 024 048 072 096 120 144 168 192 216 240 264 288 312 336 360 384; do
  curl -o "/tmp/gfs_f${HOUR}.grib2" \
    "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?\
dir=%2Fgfs.${DATE}%2F${RUN}%2Fatmos&\
file=gfs.t${RUN}z.pgrb2.0p25.f${HOUR}&\
var_TMP=on&var_RH=on&var_UGRD=on&var_VGRD=on&var_APCP=on&var_TCDC=on&var_PRMSL=on&\
lev_2_m_above_ground=on&lev_10_m_above_ground=on&lev_surface=on&lev_entire_atmosphere=on&\
subregion=&toplat=63&leftlon=22&rightlon=25&bottomlat=60"
done
```

## DuckDB Queries

### Current Temperature

```sql
SELECT
    round(value - 273.15, 1) as temp_celsius,
    forecast_time as hours_ahead
FROM read_grib('/tmp/gfs_f000.grib2')
WHERE parameter = 'Temperature'
  AND surface = 'Height_Above_Ground'
  AND latitude BETWEEN 61.25 AND 61.75
  AND longitude BETWEEN 23.5 AND 24.0
LIMIT 1;
```

### 16-Day Temperature Forecast

```sql
-- Combine multiple forecast files
SELECT
    forecast_time as hours,
    round(value - 273.15, 1) as temp_c
FROM read_grib('/tmp/gfs_f*.grib2')
WHERE parameter = 'Temperature'
  AND surface = 'Height_Above_Ground'
  AND abs(latitude - 61.5) < 0.25
  AND abs(longitude - 23.8) < 0.25
ORDER BY forecast_time;
```

### Wind Speed and Direction

```sql
WITH winds AS (
    SELECT
        forecast_time,
        parameter,
        value
    FROM read_grib('/tmp/gfs_f024.grib2')
    WHERE parameter IN ('U_Wind', 'V_Wind')
      AND surface = 'Height_Above_Ground'
      AND abs(latitude - 61.5) < 0.25
      AND abs(longitude - 23.8) < 0.25
)
SELECT
    forecast_time,
    round(sqrt(pow(u.value, 2) + pow(v.value, 2)), 1) as wind_speed_ms,
    round(degrees(atan2(-u.value, -v.value)) + 180, 0) as wind_direction_deg
FROM winds u
JOIN winds v USING (forecast_time)
WHERE u.parameter = 'U_Wind' AND v.parameter = 'V_Wind';
```

### Precipitation Accumulation

```sql
SELECT
    forecast_time as hours,
    round(value, 1) as precip_mm
FROM read_grib('/tmp/gfs_f*.grib2')
WHERE parameter = 'Total_Precip'
  AND abs(latitude - 61.5) < 0.25
  AND abs(longitude - 23.8) < 0.25
ORDER BY forecast_time;
```

### Cloud Cover

```sql
SELECT
    forecast_time,
    round(value, 0) as cloud_pct
FROM read_grib('/tmp/gfs_f024.grib2')
WHERE parameter = 'Cloud_Cover'
  AND surface = 'Entire_Atmosphere'
  AND abs(latitude - 61.5) < 0.25
ORDER BY forecast_time;
```

## Full 16-Day Forecast Script

```bash
#!/usr/bin/env bash
# fetch-tampere-forecast.sh

set -e

DATE=$(date -u +%Y%m%d)
RUN=00
OUTDIR=/tmp/gfs_tampere

mkdir -p "$OUTDIR"

echo "Downloading GFS forecast for $DATE ${RUN}z..."

HOURS=(000 006 012 018 024 036 048 060 072 084 096 120 144 168 192 216 240 264 288 312 336 360 384)

for H in "${HOURS[@]}"; do
  echo "  Fetching f${H}..."
  curl -s -o "${OUTDIR}/f${H}.grib2" \
    "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2Fgfs.${DATE}%2F${RUN}%2Fatmos&file=gfs.t${RUN}z.pgrb2.0p25.f${H}&var_TMP=on&var_APCP=on&var_UGRD=on&var_VGRD=on&lev_2_m_above_ground=on&lev_10_m_above_ground=on&lev_surface=on&subregion=&toplat=63&leftlon=22&rightlon=25&bottomlat=60"
done

echo "Done. Query with:"
echo "  ./build/release/duckdb -c \"SELECT * FROM read_grib('${OUTDIR}/*.grib2') LIMIT 10\""
```

## Parameter Mapping (GRIB2 → Human Readable)

The extension maps these automatically:

| GRIB2 Code | DuckDB ENUM |
| ---------- | ----------- |
| discipline=0, cat=0, num=0 | Temperature |
| discipline=0, cat=1, num=1 | Relative_Humidity |
| discipline=0, cat=1, num=8 | Total_Precip |
| discipline=0, cat=2, num=2 | U_Wind |
| discipline=0, cat=2, num=3 | V_Wind |
| discipline=0, cat=2, num=22 | Wind_Gust |
| discipline=0, cat=6, num=1 | Cloud_Cover |
| discipline=0, cat=3, num=1 | Pressure_MSL |

## Notes

- GFS data retained ~10 days on NOAA servers
- New runs available ~3.5h after model time
- Precipitation is accumulated from forecast start
- Use `forecast_time` column to identify hours ahead
- Wind direction: 0°=N, 90°=E, 180°=S, 270°=W
