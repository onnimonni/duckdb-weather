#include "weather_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include <cmath>

namespace duckdb {

// ============ Weather Utility Macros ============

void RegisterWeatherMacros(ExtensionLoader &loader) {
  auto &db = loader.GetDatabaseInstance();
  Connection conn(db);

  // Temperature conversions
  conn.Query(R"(
		CREATE OR REPLACE MACRO kelvin_to_celsius(k) AS k - 273.15
	)");
  conn.Query(R"(
		CREATE OR REPLACE MACRO celsius_to_fahrenheit(c) AS c * 9.0/5.0 + 32
	)");
  conn.Query(R"(
		CREATE OR REPLACE MACRO kelvin_to_fahrenheit(k) AS (k - 273.15) * 9.0/5.0 + 32
	)");
  conn.Query(R"(
		CREATE OR REPLACE MACRO fahrenheit_to_celsius(f) AS (f - 32) * 5.0/9.0
	)");

  // Wind calculations
  conn.Query(R"(
		CREATE OR REPLACE MACRO wind_speed(u, v) AS sqrt(u*u + v*v)
	)");
  conn.Query(R"(
		CREATE OR REPLACE MACRO wind_direction(u, v) AS
			CASE
				WHEN u = 0 AND v = 0 THEN NULL
				ELSE (degrees(atan2(-u, -v)) + 360) % 360
			END
	)");
  conn.Query(R"(
		CREATE OR REPLACE MACRO wind_speed_kmh(ms) AS ms * 3.6
	)");
  conn.Query(R"(
		CREATE OR REPLACE MACRO wind_speed_mph(ms) AS ms * 2.237
	)");
  conn.Query(R"(
		CREATE OR REPLACE MACRO wind_speed_knots(ms) AS ms * 1.944
	)");

  // Pressure conversion
  conn.Query(R"(
		CREATE OR REPLACE MACRO pa_to_hpa(pa) AS pa / 100.0
	)");
  conn.Query(R"(
		CREATE OR REPLACE MACRO hpa_to_inhg(hpa) AS hpa * 0.02953
	)");
  conn.Query(R"(
		CREATE OR REPLACE MACRO inhg_to_hpa(inhg) AS inhg / 0.02953
	)");

  // Dew point calculation (Magnus formula approximation)
  conn.Query(R"(
		CREATE OR REPLACE MACRO dew_point(temp_c, rh) AS
			243.04 * (ln(rh/100.0) + (17.625 * temp_c)/(243.04 + temp_c)) /
			(17.625 - ln(rh/100.0) - (17.625 * temp_c)/(243.04 + temp_c))
	)");

  // Heat index (simplified Rothfusz regression)
  // Valid for temps >= 27°C and RH >= 40%
  conn.Query(R"(
		CREATE OR REPLACE MACRO heat_index(temp_c, rh) AS
			CASE
				WHEN temp_c < 27 THEN temp_c
				ELSE -8.785 + 1.611*temp_c + 2.339*rh - 0.146*temp_c*rh
					- 0.013*temp_c*temp_c - 0.016*rh*rh + 0.002*temp_c*temp_c*rh
					+ 0.001*temp_c*rh*rh - 0.000002*temp_c*temp_c*rh*rh
			END
	)");

  // Wind chill (Environment Canada formula)
  // Valid for temps <= 10°C and wind >= 4.8 km/h
  conn.Query(R"(
		CREATE OR REPLACE MACRO wind_chill(temp_c, wind_kmh) AS
			CASE
				WHEN temp_c > 10 OR wind_kmh < 4.8 THEN temp_c
				ELSE 13.12 + 0.6215*temp_c - 11.37*power(wind_kmh, 0.16)
					+ 0.3965*temp_c*power(wind_kmh, 0.16)
			END
	)");

  // Apparent temperature (feels like) - combines heat index and wind chill
  conn.Query(R"(
		CREATE OR REPLACE MACRO feels_like(temp_c, rh, wind_kmh) AS
			CASE
				WHEN temp_c >= 27 AND rh >= 40 THEN heat_index(temp_c, rh)
				WHEN temp_c <= 10 AND wind_kmh >= 4.8 THEN wind_chill(temp_c, wind_kmh)
				ELSE temp_c
			END
	)");

  // Beaufort scale (wind force 0-12)
  conn.Query(R"(
		CREATE OR REPLACE MACRO beaufort_scale(wind_ms) AS
			CASE
				WHEN wind_ms < 0.5 THEN 0   -- Calm
				WHEN wind_ms < 1.6 THEN 1   -- Light air
				WHEN wind_ms < 3.4 THEN 2   -- Light breeze
				WHEN wind_ms < 5.5 THEN 3   -- Gentle breeze
				WHEN wind_ms < 8.0 THEN 4   -- Moderate breeze
				WHEN wind_ms < 10.8 THEN 5  -- Fresh breeze
				WHEN wind_ms < 13.9 THEN 6  -- Strong breeze
				WHEN wind_ms < 17.2 THEN 7  -- High wind
				WHEN wind_ms < 20.8 THEN 8  -- Gale
				WHEN wind_ms < 24.5 THEN 9  -- Strong gale
				WHEN wind_ms < 28.5 THEN 10 -- Storm
				WHEN wind_ms < 32.7 THEN 11 -- Violent storm
				ELSE 12                      -- Hurricane
			END
	)");

  // Beaufort description
  conn.Query(R"(
		CREATE OR REPLACE MACRO beaufort_description(wind_ms) AS
			CASE
				WHEN wind_ms < 0.5 THEN 'Calm'
				WHEN wind_ms < 1.6 THEN 'Light air'
				WHEN wind_ms < 3.4 THEN 'Light breeze'
				WHEN wind_ms < 5.5 THEN 'Gentle breeze'
				WHEN wind_ms < 8.0 THEN 'Moderate breeze'
				WHEN wind_ms < 10.8 THEN 'Fresh breeze'
				WHEN wind_ms < 13.9 THEN 'Strong breeze'
				WHEN wind_ms < 17.2 THEN 'High wind'
				WHEN wind_ms < 20.8 THEN 'Gale'
				WHEN wind_ms < 24.5 THEN 'Strong gale'
				WHEN wind_ms < 28.5 THEN 'Storm'
				WHEN wind_ms < 32.7 THEN 'Violent storm'
				ELSE 'Hurricane'
			END
	)");

  // Visibility category
  conn.Query(R"(
		CREATE OR REPLACE MACRO visibility_category(vis_m) AS
			CASE
				WHEN vis_m < 100 THEN 'Dense fog'
				WHEN vis_m < 1000 THEN 'Fog'
				WHEN vis_m < 4000 THEN 'Mist'
				WHEN vis_m < 10000 THEN 'Haze'
				ELSE 'Clear'
			END
	)");

  // Precipitation intensity (rain rate)
  conn.Query(R"(
		CREATE OR REPLACE MACRO precip_intensity(rate_mm_h) AS
			CASE
				WHEN rate_mm_h = 0 THEN 'None'
				WHEN rate_mm_h < 2.5 THEN 'Light'
				WHEN rate_mm_h < 7.6 THEN 'Moderate'
				WHEN rate_mm_h < 50 THEN 'Heavy'
				ELSE 'Violent'
			END
	)");

  // Cloud cover description
  conn.Query(R"(
		CREATE OR REPLACE MACRO cloud_description(cover_pct) AS
			CASE
				WHEN cover_pct < 12.5 THEN 'Clear'
				WHEN cover_pct < 25 THEN 'Mostly clear'
				WHEN cover_pct < 50 THEN 'Partly cloudy'
				WHEN cover_pct < 87.5 THEN 'Mostly cloudy'
				ELSE 'Overcast'
			END
	)");

  // UV index category
  conn.Query(R"(
		CREATE OR REPLACE MACRO uv_category(uv_index) AS
			CASE
				WHEN uv_index < 3 THEN 'Low'
				WHEN uv_index < 6 THEN 'Moderate'
				WHEN uv_index < 8 THEN 'High'
				WHEN uv_index < 11 THEN 'Very high'
				ELSE 'Extreme'
			END
	)");

  // Cardinal direction from degrees
  conn.Query(R"(
		CREATE OR REPLACE MACRO cardinal_direction(degrees) AS
			CASE
				WHEN degrees IS NULL THEN NULL
				WHEN degrees < 22.5 OR degrees >= 337.5 THEN 'N'
				WHEN degrees < 67.5 THEN 'NE'
				WHEN degrees < 112.5 THEN 'E'
				WHEN degrees < 157.5 THEN 'SE'
				WHEN degrees < 202.5 THEN 'S'
				WHEN degrees < 247.5 THEN 'SW'
				WHEN degrees < 292.5 THEN 'W'
				ELSE 'NW'
			END
	)");

  // Distance in meters to km or miles
  conn.Query(R"(
		CREATE OR REPLACE MACRO meters_to_km(m) AS m / 1000.0
	)");
  conn.Query(R"(
		CREATE OR REPLACE MACRO meters_to_miles(m) AS m / 1609.344
	)");
  conn.Query(R"(
		CREATE OR REPLACE MACRO meters_to_feet(m) AS m * 3.28084
	)");

  // Precipitation mm to inches
  conn.Query(R"(
		CREATE OR REPLACE MACRO mm_to_inches(mm) AS mm / 25.4
	)");
}

void RegisterWeatherFunction(ExtensionLoader &loader) {
  // Register macros
  RegisterWeatherMacros(loader);
}

} // namespace duckdb
