#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

void RegisterGfsForecastFunction(ExtensionLoader &loader);
void OptimizeGfsForecastLimitPushdown(unique_ptr<LogicalOperator> &op);

} // namespace duckdb
