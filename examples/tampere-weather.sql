-- Tampere, Finland Weather from GFS Forecast
-- Run: ./build/release/duckdb -f examples/tampere-weather.sql
--
-- First download fresh GFS data:
-- curl -o /tmp/gfs_temp.grib2 "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?dir=%2Fgfs.$(date -u +%Y%m%d)%2F00%2Fatmos&file=gfs.t00z.pgrb2.0p25.f000&var_TMP=on&lev_2_m_above_ground=on&subregion=&toplat=70&leftlon=15&rightlon=35&bottomlat=55"

.timer on

SELECT
    '🌡️ Tampere Weather' as report,
    round(value - 273.15, 1) as temperature_celsius,
    parameter,
    surface,
    surface_value as height_m,
    latitude,
    longitude
FROM read_grib('/tmp/gfs_temp.grib2')
WHERE latitude BETWEEN 61.0 AND 62.0
  AND longitude BETWEEN 23.0 AND 25.0
ORDER BY abs(latitude - 61.5) + abs(longitude - 23.8)
LIMIT 1;
