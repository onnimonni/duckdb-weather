#!/usr/bin/env bash
# Fetch latest GFS forecast and show Tampere temperature

set -e

DATE=$(date -u +%Y%m%d)
echo "Fetching GFS forecast for $DATE..."

curl -s -o /tmp/gfs_temp.grib2 \
  "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2Fgfs.${DATE}%2F00%2Fatmos&file=gfs.t00z.pgrb2.0p25.f000&var_TMP=on&lev_2_m_above_ground=on&subregion=&toplat=70&leftlon=15&rightlon=35&bottomlat=55"

./build/release/duckdb -f examples/tampere-weather.sql
