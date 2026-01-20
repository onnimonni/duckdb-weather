#include "gfs_forecast_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "grib2_ffi.h"
#include <atomic>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

// ============================================================
// Variable and Level mappings
// ============================================================

// Map human-readable variable names to GFS API parameters
static const std::unordered_map<string, string> VARIABLE_MAP = {
    {"temperature", "var_TMP"},
    {"temp", "var_TMP"},
    {"t", "var_TMP"},
    {"humidity", "var_RH"},
    {"relative_humidity", "var_RH"},
    {"rh", "var_RH"},
    {"wind_u", "var_UGRD"},
    {"u_wind", "var_UGRD"},
    {"ugrd", "var_UGRD"},
    {"wind_v", "var_VGRD"},
    {"v_wind", "var_VGRD"},
    {"vgrd", "var_VGRD"},
    {"precipitation", "var_APCP"},
    {"precip", "var_APCP"},
    {"rain", "var_APCP"},
    {"apcp", "var_APCP"},
    {"gust", "var_GUST"},
    {"wind_gust", "var_GUST"},
    {"clouds", "var_TCDC"},
    {"cloud_cover", "var_TCDC"},
    {"tcdc", "var_TCDC"},
    {"pressure", "var_PRMSL"},
    {"msl_pressure", "var_PRMSL"},
    {"prmsl", "var_PRMSL"},
};

// Map human-readable level names to GFS API parameters
static const std::unordered_map<string, string> LEVEL_MAP = {
    {"2m", "lev_2_m_above_ground"},
    {"2_m", "lev_2_m_above_ground"},
    {"2m_above_ground", "lev_2_m_above_ground"},
    {"10m", "lev_10_m_above_ground"},
    {"10_m", "lev_10_m_above_ground"},
    {"10m_above_ground", "lev_10_m_above_ground"},
    {"surface", "lev_surface"},
    {"sfc", "lev_surface"},
    {"atmosphere", "lev_entire_atmosphere"},
    {"entire_atmosphere", "lev_entire_atmosphere"},
    {"msl", "lev_mean_sea_level"},
    {"mean_sea_level", "lev_mean_sea_level"},
};

// ============================================================
// Bind Data - stores pushed-down filters
// ============================================================

struct GfsForecastBindData : public TableFunctionData {
  // Output schema
  vector<string> column_names;
  vector<LogicalType> column_types;

  // Pushed-down filters (from WHERE clause)
  string run_date;                // YYYYMMDD format
  int32_t run_hour = -1;          // 0, 6, 12, 18
  vector<int32_t> forecast_hours; // f000, f003, etc.
  vector<string> variables;       // var_TMP, var_RH, etc.
  vector<string> levels;          // lev_2_m_above_ground, etc.

  // Bounding box (subregion)
  double lat_min = -90.0;
  double lat_max = 90.0;
  double lon_min = 0.0;
  double lon_max = 360.0;
  bool has_bbox = false;

  // LIMIT pushdown
  idx_t max_results = 0; // 0 = unlimited
};

// ============================================================
// Global State
// ============================================================

struct GfsForecastGlobalState : public GlobalTableFunctionState {
  Grib2Reader *reader = nullptr;
  string http_data; // Keep HTTP data alive
  bool finished = false;
  idx_t rows_returned = 0;

  // Multi-forecast-hour support
  idx_t current_fhour_idx = 0;
  bool current_file_initialized = false;

  // Progress tracking (using fine-grained progress within files)
  std::atomic<idx_t> total_files{0};
  std::atomic<idx_t> completed_files{0};
  std::atomic<int> current_file_progress{0}; // 0-100 within current file

  idx_t MaxThreads() const override { return 1; }

  ~GfsForecastGlobalState() {
    if (reader) {
      grib2_close(reader);
      reader = nullptr;
    }
  }

  void CloseCurrentReader() {
    if (reader) {
      grib2_close(reader);
      reader = nullptr;
    }
    http_data.clear();
    current_file_initialized = false;
  }
};

// ============================================================
// URL Builder
// ============================================================

static string BuildGfsUrl(const GfsForecastBindData &bind_data,
                          int32_t forecast_hour) {
  string url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?";

  // Directory: /gfs.YYYYMMDD/HH/atmos
  string run_hour_str = StringUtil::Format(
      "%02d", bind_data.run_hour >= 0 ? bind_data.run_hour : 0);
  url += "dir=%2Fgfs." + bind_data.run_date + "%2F" + run_hour_str + "%2Fatmos";

  // File: gfs.tHHz.pgrb2.0p25.fFFF
  string fhour_str = StringUtil::Format("%03d", forecast_hour);
  url += "&file=gfs.t" + run_hour_str + "z.pgrb2.0p25.f" + fhour_str;

  // Variables
  if (bind_data.variables.empty()) {
    // Default variables
    url += "&var_TMP=on&var_RH=on&var_UGRD=on&var_VGRD=on";
  } else {
    for (const auto &var : bind_data.variables) {
      url += "&" + var + "=on";
    }
  }

  // Levels
  if (bind_data.levels.empty()) {
    // Default levels
    url += "&lev_2_m_above_ground=on&lev_10_m_above_ground=on&lev_surface=on";
  } else {
    for (const auto &lev : bind_data.levels) {
      url += "&" + lev + "=on";
    }
  }

  // Subregion (bounding box)
  url += "&subregion=";
  url += "&toplat=" + std::to_string(static_cast<int>(bind_data.lat_max));
  url += "&bottomlat=" + std::to_string(static_cast<int>(bind_data.lat_min));
  url += "&leftlon=" + std::to_string(static_cast<int>(bind_data.lon_min));
  url += "&rightlon=" + std::to_string(static_cast<int>(bind_data.lon_max));

  return url;
}

// ============================================================
// HTTP Fetch using DuckDB's HTTPUtil
// ============================================================

static string FetchGribData(ClientContext &context, const string &url) {
  auto &http_util = HTTPUtil::Get(*context.db);
  auto params = http_util.InitializeParameters(context, url);

  HTTPHeaders headers;
  GetRequestInfo get_request(url, headers, *params, nullptr, nullptr);

  auto response = http_util.Request(get_request);

  if (!response->Success()) {
    throw IOException("GFS API returned status %d for URL: %s",
                      static_cast<int32_t>(response->status), url);
  }

  return response->body;
}

// ============================================================
// Bind Function
// ============================================================

static unique_ptr<FunctionData>
GfsForecastBind(ClientContext &context, TableFunctionBindInput &input,
                vector<LogicalType> &return_types, vector<string> &names) {
  auto bind_data = make_uniq<GfsForecastBindData>();

  // Define output schema
  bind_data->column_names = {"latitude",      "longitude", "value",
                             "unit",          "variable",  "level",
                             "forecast_hour", "run_date",  "run_hour"};

  return_types = {
      LogicalType::DOUBLE,  // latitude
      LogicalType::DOUBLE,  // longitude
      LogicalType::DOUBLE,  // value
      LogicalType::VARCHAR, // unit
      LogicalType::VARCHAR, // variable
      LogicalType::VARCHAR, // level
      LogicalType::INTEGER, // forecast_hour
      LogicalType::VARCHAR, // run_date
      LogicalType::INTEGER  // run_hour
  };

  names = bind_data->column_names;

  // Set defaults: today's date, run hour 00, forecast hour 0
  auto now = Timestamp::GetCurrentTimestamp();
  auto date = Timestamp::GetDate(now);
  int32_t year, month, day;
  Date::Convert(date, year, month, day);
  bind_data->run_date = StringUtil::Format("%04d%02d%02d", year, month, day);
  bind_data->run_hour = 0;
  bind_data->forecast_hours.push_back(0);

  return std::move(bind_data);
}

// ============================================================
// Filter Pushdown Handler
// ============================================================

static string NormalizeVariableName(const string &input) {
  string lower = StringUtil::Lower(input);
  auto it = VARIABLE_MAP.find(lower);
  if (it != VARIABLE_MAP.end()) {
    return it->second;
  }
  // If already in API format (var_XXX), return as-is
  if (StringUtil::StartsWith(lower, "var_")) {
    return StringUtil::Upper(lower);
  }
  return "";
}

static string NormalizeLevelName(const string &input) {
  string lower = StringUtil::Lower(input);
  auto it = LEVEL_MAP.find(lower);
  if (it != LEVEL_MAP.end()) {
    return it->second;
  }
  // If already in API format (lev_XXX), return as-is
  if (StringUtil::StartsWith(lower, "lev_")) {
    return lower;
  }
  return "";
}

static void GfsForecastPushdownFilter(ClientContext &context, LogicalGet &get,
                                      FunctionData *bind_data_p,
                                      vector<unique_ptr<Expression>> &filters) {
  auto &bind_data = bind_data_p->Cast<GfsForecastBindData>();

  // Build column name → index map
  std::unordered_map<string, idx_t> column_map;
  for (idx_t i = 0; i < bind_data.column_names.size(); i++) {
    column_map[bind_data.column_names[i]] = i;
  }

  vector<idx_t> filters_to_remove;

  for (idx_t i = 0; i < filters.size(); i++) {
    auto &filter = filters[i];

    // Handle IN clauses for variables and levels
    if (filter->GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
      auto &op = filter->Cast<BoundOperatorExpression>();

      if (op.children.size() >= 2 && op.children[0]->GetExpressionClass() ==
                                         ExpressionClass::BOUND_COLUMN_REF) {

        auto &col_ref = op.children[0]->Cast<BoundColumnRefExpression>();
        string col_name = col_ref.GetName();

        // variable IN ('temperature', 'humidity', ...)
        if (col_name == "variable") {
          vector<string> vars;
          bool all_valid = true;

          for (size_t j = 1; j < op.children.size(); j++) {
            if (op.children[j]->GetExpressionClass() ==
                ExpressionClass::BOUND_CONSTANT) {
              auto &constant = op.children[j]->Cast<BoundConstantExpression>();
              if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
                string var = NormalizeVariableName(constant.value.ToString());
                if (!var.empty()) {
                  vars.push_back(var);
                  continue;
                }
              }
            }
            all_valid = false;
            break;
          }

          if (all_valid && !vars.empty()) {
            bind_data.variables = vars;
            filters_to_remove.push_back(i);
            continue;
          }
        }

        // level IN ('2m', 'surface', ...)
        if (col_name == "level") {
          vector<string> levs;
          bool all_valid = true;

          for (size_t j = 1; j < op.children.size(); j++) {
            if (op.children[j]->GetExpressionClass() ==
                ExpressionClass::BOUND_CONSTANT) {
              auto &constant = op.children[j]->Cast<BoundConstantExpression>();
              if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
                string lev = NormalizeLevelName(constant.value.ToString());
                if (!lev.empty()) {
                  levs.push_back(lev);
                  continue;
                }
              }
            }
            all_valid = false;
            break;
          }

          if (all_valid && !levs.empty()) {
            bind_data.levels = levs;
            filters_to_remove.push_back(i);
            continue;
          }
        }

        // forecast_hour IN (0, 6, 12, ...)
        if (col_name == "forecast_hour") {
          vector<int32_t> hours;
          bool all_valid = true;

          for (size_t j = 1; j < op.children.size(); j++) {
            if (op.children[j]->GetExpressionClass() ==
                ExpressionClass::BOUND_CONSTANT) {
              auto &constant = op.children[j]->Cast<BoundConstantExpression>();
              if (constant.value.type().IsIntegral()) {
                hours.push_back(constant.value.GetValue<int32_t>());
                continue;
              }
            }
            all_valid = false;
            break;
          }

          if (all_valid && !hours.empty()) {
            bind_data.forecast_hours = hours;
            filters_to_remove.push_back(i);
            continue;
          }
        }
      }
    }

    // Handle comparison expressions (=, >=, <=)
    if (filter->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
      auto &comparison = filter->Cast<BoundComparisonExpression>();

      if (comparison.left->GetExpressionClass() ==
              ExpressionClass::BOUND_COLUMN_REF &&
          comparison.right->GetExpressionClass() ==
              ExpressionClass::BOUND_CONSTANT) {

        auto &col_ref = comparison.left->Cast<BoundColumnRefExpression>();
        auto &constant = comparison.right->Cast<BoundConstantExpression>();
        string col_name = col_ref.GetName();

        // run_date = '2026-01-20' or run_date = DATE '2026-01-20'
        if (col_name == "run_date" &&
            filter->type == ExpressionType::COMPARE_EQUAL) {
          string date_str;
          if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
            date_str = constant.value.ToString();
          } else if (constant.value.type().id() == LogicalTypeId::DATE) {
            auto date_val = constant.value.GetValue<date_t>();
            int32_t year, month, day;
            Date::Convert(date_val, year, month, day);
            date_str = StringUtil::Format("%04d%02d%02d", year, month, day);
          }
          if (!date_str.empty()) {
            // Remove dashes if present
            date_str.erase(std::remove(date_str.begin(), date_str.end(), '-'),
                           date_str.end());
            bind_data.run_date = date_str;
            filters_to_remove.push_back(i);
            continue;
          }
        }

        // run_hour = 0 (or 6, 12, 18)
        if (col_name == "run_hour" &&
            filter->type == ExpressionType::COMPARE_EQUAL) {
          if (constant.value.type().IsIntegral()) {
            bind_data.run_hour = constant.value.GetValue<int32_t>();
            filters_to_remove.push_back(i);
            continue;
          }
        }

        // forecast_hour = 24
        if (col_name == "forecast_hour" &&
            filter->type == ExpressionType::COMPARE_EQUAL) {
          if (constant.value.type().IsIntegral()) {
            bind_data.forecast_hours.clear();
            bind_data.forecast_hours.push_back(
                constant.value.GetValue<int32_t>());
            filters_to_remove.push_back(i);
            continue;
          }
        }

        // variable = 'temperature'
        if (col_name == "variable" &&
            filter->type == ExpressionType::COMPARE_EQUAL) {
          if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
            string var = NormalizeVariableName(constant.value.ToString());
            if (!var.empty()) {
              bind_data.variables.clear();
              bind_data.variables.push_back(var);
              filters_to_remove.push_back(i);
              continue;
            }
          }
        }

        // level = '2m'
        if (col_name == "level" &&
            filter->type == ExpressionType::COMPARE_EQUAL) {
          if (constant.value.type().id() == LogicalTypeId::VARCHAR) {
            string lev = NormalizeLevelName(constant.value.ToString());
            if (!lev.empty()) {
              bind_data.levels.clear();
              bind_data.levels.push_back(lev);
              filters_to_remove.push_back(i);
              continue;
            }
          }
        }

        // Bounding box filters
        // latitude >= X → lat_min
        if (col_name == "latitude") {
          if (constant.value.type().IsNumeric()) {
            double val = constant.value.GetValue<double>();
            if (filter->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
                filter->type == ExpressionType::COMPARE_GREATERTHAN) {
              bind_data.lat_min = val;
              bind_data.has_bbox = true;
              // Don't remove - DuckDB applies for exact filtering
              continue;
            }
            if (filter->type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
                filter->type == ExpressionType::COMPARE_LESSTHAN) {
              bind_data.lat_max = val;
              bind_data.has_bbox = true;
              continue;
            }
          }
        }

        // longitude >= X → lon_min
        if (col_name == "longitude") {
          if (constant.value.type().IsNumeric()) {
            double val = constant.value.GetValue<double>();
            // Normalize negative longitudes to 0-360 range
            if (val < 0)
              val += 360;
            if (filter->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
                filter->type == ExpressionType::COMPARE_GREATERTHAN) {
              bind_data.lon_min = val;
              bind_data.has_bbox = true;
              continue;
            }
            if (filter->type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
                filter->type == ExpressionType::COMPARE_LESSTHAN) {
              bind_data.lon_max = val;
              bind_data.has_bbox = true;
              continue;
            }
          }
        }
      }
    }
  }

  // Remove handled filters (reverse order)
  for (auto it = filters_to_remove.rbegin(); it != filters_to_remove.rend();
       ++it) {
    filters.erase(filters.begin() + *it);
  }
}

// ============================================================
// Progress Callback
// ============================================================

static double GfsForecastProgress(ClientContext &context,
                                  const FunctionData *bind_data_p,
                                  const GlobalTableFunctionState *gstate_p) {
  auto &gstate = gstate_p->Cast<GfsForecastGlobalState>();
  idx_t total = gstate.total_files.load();
  idx_t completed = gstate.completed_files.load();
  int current_progress = gstate.current_file_progress.load();

  if (total <= 0) {
    return -1.0; // Unknown progress
  }

  // Calculate progress including partial progress of current file
  // Each file contributes (100/total)% to overall progress
  double per_file = 100.0 / static_cast<double>(total);
  double base_progress = static_cast<double>(completed) * per_file;
  double current_file_contribution =
      (static_cast<double>(current_progress) / 100.0) * per_file;

  return base_progress + current_file_contribution;
}

// ============================================================
// Init Global
// ============================================================

static unique_ptr<GlobalTableFunctionState>
GfsForecastInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<GfsForecastBindData>();
  auto state = make_uniq<GfsForecastGlobalState>();

  // Set total files for progress tracking
  state->total_files = bind_data.forecast_hours.size();

  return std::move(state);
}

// ============================================================
// Scan Function
// ============================================================

// Map GRIB parameter codes to human-readable names
static string ParameterCodeToName(uint8_t discipline, uint8_t category,
                                  uint8_t number) {
  if (discipline == 0) { // Meteorological
    if (category == 0) { // Temperature
      if (number == 0)
        return "temperature";
    }
    if (category == 1) { // Moisture
      if (number == 1)
        return "humidity";
      if (number == 8)
        return "precipitation";
    }
    if (category == 2) { // Momentum
      if (number == 2)
        return "wind_u";
      if (number == 3)
        return "wind_v";
      if (number == 22)
        return "gust";
    }
    if (category == 3) { // Mass
      if (number == 1)
        return "pressure";
    }
    if (category == 6) { // Cloud
      if (number == 1)
        return "clouds";
    }
  }
  return "unknown";
}

// Map GRIB surface codes to human-readable names
static string SurfaceCodeToName(uint8_t code, double value) {
  switch (code) {
  case 1:
    return "surface";
  case 10:
    return "atmosphere";
  case 100:
    return StringUtil::Format("%dhPa", static_cast<int>(value / 100));
  case 101:
    return "msl";
  case 103:
    if (value == 2)
      return "2m";
    if (value == 10)
      return "10m";
    return StringUtil::Format("%dm", static_cast<int>(value));
  default:
    return "unknown";
  }
}

// Get unit for a variable name (empty string means NULL)
static string GetVariableUnit(const string &variable) {
  if (variable == "temperature")
    return "K";
  if (variable == "humidity")
    return "%";
  if (variable == "wind_u" || variable == "wind_v" || variable == "gust")
    return "m/s";
  if (variable == "pressure")
    return "Pa";
  if (variable == "clouds")
    return "%";
  if (variable == "precipitation")
    return "kg/m^2";
  return ""; // Will be converted to NULL
}

static void GfsForecastScan(ClientContext &context, TableFunctionInput &data,
                            DataChunk &output) {
  auto &gstate = data.global_state->Cast<GfsForecastGlobalState>();
  auto &bind_data = data.bind_data->Cast<GfsForecastBindData>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  // Check LIMIT before fetching more data
  if (bind_data.max_results > 0 &&
      gstate.rows_returned >= bind_data.max_results) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  // Initialize or move to next forecast hour if needed
  while (!gstate.current_file_initialized) {
    if (gstate.current_fhour_idx >= bind_data.forecast_hours.size()) {
      gstate.finished = true;
      output.SetCardinality(0);
      return;
    }

    // Reset progress for new file
    gstate.current_file_progress = 0;

    int32_t fhour = bind_data.forecast_hours[gstate.current_fhour_idx];
    string url = BuildGfsUrl(bind_data, fhour);

    // Mark 10% - starting HTTP fetch
    gstate.current_file_progress = 10;

    try {
      gstate.http_data = FetchGribData(context, url);
    } catch (const std::exception &e) {
      throw IOException("Failed to fetch GFS data for fhour %d: %s", fhour,
                        e.what());
    }

    // Mark 40% - HTTP fetch complete, starting GRIB parse
    gstate.current_file_progress = 40;

    // Parse GRIB from memory
    char *error = nullptr;
    gstate.reader = grib2_open_from_bytes(
        reinterpret_cast<const uint8_t *>(gstate.http_data.data()),
        gstate.http_data.size(), &error);

    if (!gstate.reader) {
      string err_msg = error ? string(error) : "Unknown error";
      if (error)
        grib2_free_error(error);
      throw IOException("Failed to parse GRIB data for fhour %d: %s", fhour,
                        err_msg);
    }

    // Mark 50% - GRIB parsed, ready to read batches
    gstate.current_file_progress = 50;

    gstate.current_file_initialized = true;
  }

  // Get current forecast hour before reading
  int32_t fhour = bind_data.forecast_hours[gstate.current_fhour_idx];

  // Read batch from current file
  const idx_t BATCH_SIZE = STANDARD_VECTOR_SIZE;
  Grib2Batch batch = grib2_read_batch(gstate.reader, BATCH_SIZE);

  if (batch.error) {
    string err_msg(batch.error);
    grib2_free_batch(batch);
    throw IOException("GRIB read error: %s", err_msg);
  }

  // If current file exhausted, move to next forecast hour
  if (batch.count == 0) {
    grib2_free_batch(batch);
    gstate.CloseCurrentReader();
    gstate.current_fhour_idx++;
    // Recurse to fetch next file
    GfsForecastScan(context, data, output);
    return;
  }

  for (idx_t i = 0; i < batch.count; i++) {
    auto &point = batch.data[i];

    // Normalize longitude to -180 to 180
    double lon = point.longitude;
    if (lon > 180)
      lon -= 360;

    string variable = ParameterCodeToName(
        point.discipline, point.parameter_category, point.parameter_number);

    string unit = GetVariableUnit(variable);

    output.SetValue(0, i, Value(point.latitude));
    output.SetValue(1, i, Value(lon));
    output.SetValue(2, i, Value(point.value));
    output.SetValue(3, i,
                    unit.empty() ? Value(LogicalType::VARCHAR) : Value(unit));
    output.SetValue(4, i, Value(variable));
    output.SetValue(
        5, i,
        Value(SurfaceCodeToName(point.surface_type, point.surface_value)));
    output.SetValue(6, i, Value::INTEGER(fhour));
    output.SetValue(7, i, Value(bind_data.run_date));
    output.SetValue(8, i, Value::INTEGER(bind_data.run_hour));
  }

  output.SetCardinality(batch.count);
  gstate.rows_returned += batch.count;

  // Check if current file is done
  if (!batch.has_more) {
    gstate.current_file_progress = 100;
    gstate.CloseCurrentReader();
    gstate.current_fhour_idx++;
    gstate.completed_files++;
  } else {
    // Increment progress within file (50% to 95% range during batch reads)
    int current = gstate.current_file_progress.load();
    if (current < 95) {
      gstate.current_file_progress = std::min(95, current + 5);
    }
  }

  grib2_free_batch(batch);

  // Check LIMIT
  if (bind_data.max_results > 0 &&
      gstate.rows_returned >= bind_data.max_results) {
    gstate.finished = true;
  }
}

// ============================================================
// Cardinality for LIMIT pushdown detection
// ============================================================

static constexpr idx_t GFS_REPORTED_CARDINALITY = 10000000;

static unique_ptr<NodeStatistics>
GfsForecastCardinality(ClientContext &context, const FunctionData *bind_data) {
  return make_uniq<NodeStatistics>(GFS_REPORTED_CARDINALITY,
                                   GFS_REPORTED_CARDINALITY);
}

// ============================================================
// LIMIT Pushdown Optimizer
// ============================================================

void OptimizeGfsForecastLimitPushdown(unique_ptr<LogicalOperator> &op) {
  if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
    auto &limit = op->Cast<LogicalLimit>();
    reference<LogicalOperator> child = *op->children[0];

    // Skip projection operators
    while (child.get().type == LogicalOperatorType::LOGICAL_PROJECTION) {
      child = *child.get().children[0];
    }

    if (child.get().type == LogicalOperatorType::LOGICAL_GET) {
      auto &get = child.get().Cast<LogicalGet>();
      if (get.function.name == "noaa_gfs_forecast_api") {
        auto &bind_data = get.bind_data->Cast<GfsForecastBindData>();

        if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
          bind_data.max_results = limit.limit_val.GetConstantValue();
        }
      }
    }
  }

  // Recurse
  for (auto &child : op->children) {
    OptimizeGfsForecastLimitPushdown(child);
  }
}

// ============================================================
// Registration
// ============================================================

void RegisterGfsForecastFunction(ExtensionLoader &loader) {
  TableFunction func("noaa_gfs_forecast_api", {}, GfsForecastScan,
                     GfsForecastBind, GfsForecastInitGlobal);
  func.pushdown_complex_filter = GfsForecastPushdownFilter;
  func.cardinality = GfsForecastCardinality;
  func.table_scan_progress = GfsForecastProgress;

  loader.RegisterFunction(func);
}

} // namespace duckdb
