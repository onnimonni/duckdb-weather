#!/usr/bin/env bash
# Fetch 16-day GFS forecast for Tampere, Finland

set -e

DATE=$(date -u +%Y%m%d)
RUN=00
OUTDIR=/tmp/gfs_tampere

mkdir -p "$OUTDIR"

echo "Downloading GFS forecast for $DATE ${RUN}z..."
echo "Region: Finland (60-63°N, 22-25°E)"
echo "Variables: Temperature, Precipitation, Wind"

HOURS=(000 006 012 018 024 036 048 060 072 084 096 120 144 168 192 216 240 264 288 312 336 360 384)

for H in "${HOURS[@]}"; do
  FILE="${OUTDIR}/f${H}.grib2"
  if [ -f "$FILE" ]; then
    echo "  f${H} exists, skipping"
    continue
  fi
  echo "  Fetching f${H}..."
  curl -s -o "$FILE" \
    "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2Fgfs.${DATE}%2F${RUN}%2Fatmos&file=gfs.t${RUN}z.pgrb2.0p25.f${H}&var_TMP=on&var_APCP=on&var_UGRD=on&var_VGRD=on&lev_2_m_above_ground=on&lev_10_m_above_ground=on&lev_surface=on&subregion=&toplat=63&leftlon=22&rightlon=25&bottomlat=60"
done

echo ""
echo "Done! Files in: $OUTDIR"
echo ""
echo "Example queries:"
echo ""
echo "# Temperature forecast"
echo "./build/release/duckdb -c \""
echo "SELECT"
echo "    forecast_time as hours,"
echo "    round(value - 273.15, 1) as temp_c"
echo "FROM read_grib('${OUTDIR}/*.grib2')"
echo "WHERE parameter = 'Temperature'"
echo "  AND surface = 'Height_Above_Ground'"
echo "  AND abs(latitude - 61.5) < 0.2"
echo "  AND abs(longitude - 23.8) < 0.2"
echo "ORDER BY forecast_time"
echo "\""
