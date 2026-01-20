#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

void RegisterMetForecastFunction(ExtensionLoader &loader);

} // namespace duckdb
