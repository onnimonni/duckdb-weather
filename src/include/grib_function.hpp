#pragma once

#include "duckdb.hpp"

namespace duckdb {

// Register the read_grib() table function
void RegisterGribFunction(ExtensionLoader &loader);

// Register GRIB2 ENUM types
void RegisterGribEnumTypes(DatabaseInstance &db);

} // namespace duckdb
