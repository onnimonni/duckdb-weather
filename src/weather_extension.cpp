#define DUCKDB_EXTENSION_MAIN

#include "weather_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "gfs_forecast_function.hpp"
#include "grib_function.hpp"
#include "weather_function.hpp"

namespace duckdb {

// Optimizer for LIMIT pushdown
static void WeatherOptimizer(OptimizerExtensionInput &input,
                             unique_ptr<LogicalOperator> &plan) {
  OptimizeGfsForecastLimitPushdown(plan);
}

static void LoadInternal(ExtensionLoader &loader) {
  auto &db = loader.GetDatabaseInstance();

  // Register GRIB2 ENUM types first
  RegisterGribEnumTypes(db);

  // Register read_grib() table function
  RegisterGribFunction(loader);

  // Register gfs_forecast() table function with filter pushdown
  RegisterGfsForecastFunction(loader);

  // Register weather utility macros (kelvin_to_celsius, wind_speed, etc.)
  RegisterWeatherFunction(loader);

  // Register optimizer extension for LIMIT pushdown
  auto &config = DBConfig::GetConfig(db);
  OptimizerExtension optimizer;
  optimizer.optimize_function = WeatherOptimizer;
  config.optimizer_extensions.push_back(std::move(optimizer));
}

void WeatherExtension::Load(ExtensionLoader &loader) { LoadInternal(loader); }

std::string WeatherExtension::Name() { return "weather"; }

std::string WeatherExtension::Version() const {
#ifdef EXT_VERSION_WEATHER
  return EXT_VERSION_WEATHER;
#else
  return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(weather, loader) { duckdb::LoadInternal(loader); }
}
