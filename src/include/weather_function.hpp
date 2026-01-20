#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Register the read_weather() table function for querying H3-indexed weather
// data
void RegisterWeatherFunction(ExtensionLoader &loader);

// Register weather utility macros (temperature conversion, wind calculations,
// etc.)
void RegisterWeatherMacros(ExtensionLoader &loader);

} // namespace duckdb
