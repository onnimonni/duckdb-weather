#!/usr/bin/env bash
# Fetch GFS forecast data and ingest to H3-indexed Parquet
#
# Usage:
#   ./scripts/fetch_and_ingest.sh [REGION]
#
# Regions:
#   tampere  - Finland region (default)
#   europe   - Central Europe
#   global   - Entire globe (large!)

set -e

REGION=${1:-tampere}
DATE=$(date -u +%Y%m%d)
RUN=00  # Latest available run

# Region configurations (lat_min lat_max lon_min lon_max)
case "$REGION" in
    tampere)
        BBOX="60 63 22 25"
        ;;
    finland)
        BBOX="59 70 19 32"
        ;;
    europe)
        BBOX="35 72 -10 40"
        ;;
    *)
        echo "Unknown region: $REGION"
        exit 1
        ;;
esac

read LAT_MIN LAT_MAX LON_MIN LON_MAX <<< "$BBOX"

GRIB_DIR="/tmp/gfs_${REGION}"
OUTPUT_DIR="/tmp/weather_parquet/${REGION}"

mkdir -p "$GRIB_DIR" "$OUTPUT_DIR"

echo "=== GFS Weather Data Ingestion ==="
echo "Region: $REGION ($LAT_MIN-$LAT_MAX°N, $LON_MIN-$LON_MAX°E)"
echo "Date: $DATE ${RUN}Z"
echo ""

# Forecast hours to download (covers 16 days with key intervals)
HOURS=(000 003 006 012 024 048 072 096 120 144 168 192 216 240 264 288 312 336 360 384)

echo "Step 1: Downloading GRIB2 files..."
for H in "${HOURS[@]}"; do
    FILE="${GRIB_DIR}/f${H}.grib2"
    if [ -f "$FILE" ]; then
        echo "  f${H} exists, skipping"
        continue
    fi

    echo "  Fetching f${H}..."
    URL="https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?"
    URL+="dir=%2Fgfs.${DATE}%2F${RUN}%2Fatmos&"
    URL+="file=gfs.t${RUN}z.pgrb2.0p25.f${H}&"
    URL+="var_TMP=on&var_RH=on&var_UGRD=on&var_VGRD=on&var_APCP=on&var_GUST=on&var_TCDC=on&var_PRMSL=on&"
    URL+="lev_2_m_above_ground=on&lev_10_m_above_ground=on&lev_surface=on&lev_entire_atmosphere=on&"
    URL+="subregion=&toplat=${LAT_MAX}&leftlon=${LON_MIN}&rightlon=${LON_MAX}&bottomlat=${LAT_MIN}"

    if ! curl -sf -o "$FILE" "$URL"; then
        echo "    WARNING: Failed to download f${H}, might not be available yet"
        rm -f "$FILE"
    fi
done

GRIB_COUNT=$(ls "$GRIB_DIR"/*.grib2 2>/dev/null | wc -l)
if [ "$GRIB_COUNT" -eq 0 ]; then
    echo "ERROR: No GRIB files downloaded!"
    exit 1
fi
echo "  Downloaded $GRIB_COUNT forecast files"
echo ""

echo "Step 2: Converting to H3-indexed Parquet..."
./build/release/duckdb -unsigned << EOF
-- Install and load extensions
INSTALL h3 FROM community;
LOAD h3;
LOAD './build/release/weather.duckdb_extension';

-- H3 Resolution 5 = ~8.5km edge, matches GFS 0.25° grid
SET VARIABLE h3_res = 5;

-- Ingest and transform
COPY (
    WITH raw AS (
        SELECT
            latitude, longitude, value,
            discipline, surface, parameter,
            forecast_time, surface_value
        FROM read_grib('${GRIB_DIR}/*.grib2')
        WHERE discipline = 'Meteorological'
    )
    SELECT
        h3_latlng_to_cell(latitude, longitude, getvariable('h3_res'))::UBIGINT as h3_index,
        latitude, longitude,
        CASE WHEN parameter = 'Temperature' THEN value - 273.15 ELSE value END as value,
        parameter, surface, forecast_time,
        CASE
            WHEN parameter = 'Temperature' THEN 'C'
            WHEN parameter IN ('U_Wind', 'V_Wind', 'Wind_Gust') THEN 'm/s'
            WHEN parameter = 'Pressure_MSL' THEN 'Pa'
            WHEN parameter IN ('Relative_Humidity', 'Cloud_Cover') THEN '%'
            WHEN parameter = 'Total_Precip' THEN 'mm'
            ELSE 'raw'
        END as unit,
        '${DATE}' as run_date,
        ${RUN} as run_hour
    FROM raw
    ORDER BY h3_index, forecast_time
) TO '${OUTPUT_DIR}/forecast_${DATE}_${RUN}z.parquet'
(FORMAT PARQUET, COMPRESSION ZSTD);

-- Summary
SELECT
    '${REGION}' as region,
    count(*) as total_rows,
    count(DISTINCT h3_index) as h3_cells,
    count(DISTINCT parameter) as parameters,
    min(forecast_time) || '-' || max(forecast_time) || 'h' as forecast_range
FROM read_parquet('${OUTPUT_DIR}/forecast_${DATE}_${RUN}z.parquet');
EOF

echo ""
echo "Step 3: Output files"
ls -lh "${OUTPUT_DIR}/"
echo ""

# Show sample query
echo "=== Sample Query ==="
./build/release/duckdb -unsigned << EOF
INSTALL h3 FROM community;
LOAD h3;

SELECT
    h3_cell_to_lat(h3_index)::DECIMAL(6,3) as lat,
    h3_cell_to_lng(h3_index)::DECIMAL(6,3) as lng,
    parameter,
    round(value, 1) as value,
    unit,
    forecast_time as hours
FROM read_parquet('${OUTPUT_DIR}/forecast_${DATE}_${RUN}z.parquet')
WHERE parameter = 'Temperature'
  AND h3_cell_to_lat(h3_index) BETWEEN 61.4 AND 61.6
  AND h3_cell_to_lng(h3_index) BETWEEN 23.7 AND 24.0
ORDER BY forecast_time
LIMIT 10;
EOF

echo ""
echo "Done! Parquet files ready in: ${OUTPUT_DIR}/"
echo ""
echo "Next: Upload to WebDAV with:"
echo "  ./scripts/upload_to_webdav.sh ${REGION}"
