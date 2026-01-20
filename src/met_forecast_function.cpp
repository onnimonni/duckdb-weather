#include "met_forecast_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "yyjson.hpp"
#include <vector>

using namespace duckdb_yyjson;

namespace duckdb {

// ============================================================
// Configuration key for User-Agent
// ============================================================

static constexpr const char *MET_USER_AGENT_KEY = "met_user_agent";
static constexpr const char *DEFAULT_USER_AGENT =
    "duckdb-weather/0.1 github.com/onnimonni/duckdb-weather";

// ============================================================
// Parsed forecast data point
// ============================================================

struct MetForecastPoint {
  string time;
  double air_temperature;
  double relative_humidity;
  double wind_speed;
  double wind_from_direction;
  double wind_speed_of_gust;
  double air_pressure_at_sea_level;
  double cloud_area_fraction;
  double precipitation_amount;
};

// ============================================================
// Bind Data
// ============================================================

struct MetForecastBindData : public TableFunctionData {
  double latitude = 0.0;
  double longitude = 0.0;
  double altitude = -1.0; // Optional, -1 means not set
  string user_agent;

  vector<string> column_names;
  vector<LogicalType> column_types;
};

// ============================================================
// Global State
// ============================================================

struct MetForecastGlobalState : public GlobalTableFunctionState {
  vector<MetForecastPoint> data_points;
  idx_t current_idx = 0;
  bool finished = false;
  double latitude = 0.0;
  double longitude = 0.0;

  idx_t MaxThreads() const override { return 1; }
};

// ============================================================
// JSON Parser using yyjson
// ============================================================

static double GetJsonDouble(yyjson_val *obj, const char *key,
                            double default_val = 0.0) {
  yyjson_val *val = yyjson_obj_get(obj, key);
  if (val && yyjson_is_num(val)) {
    return yyjson_get_num(val);
  }
  return default_val;
}

static string GetJsonString(yyjson_val *obj, const char *key) {
  yyjson_val *val = yyjson_obj_get(obj, key);
  if (val && yyjson_is_str(val)) {
    return string(yyjson_get_str(val));
  }
  return "";
}

static vector<MetForecastPoint> ParseMetJson(const string &json_data) {
  vector<MetForecastPoint> points;

  yyjson_doc *doc = yyjson_read(json_data.c_str(), json_data.size(), 0);
  if (!doc) {
    throw InvalidInputException("Failed to parse MET API JSON response");
  }

  yyjson_val *root = yyjson_doc_get_root(doc);
  yyjson_val *properties = yyjson_obj_get(root, "properties");
  if (!properties) {
    yyjson_doc_free(doc);
    throw InvalidInputException("MET API response missing 'properties'");
  }

  yyjson_val *timeseries = yyjson_obj_get(properties, "timeseries");
  if (!timeseries || !yyjson_is_arr(timeseries)) {
    yyjson_doc_free(doc);
    throw InvalidInputException("MET API response missing 'timeseries' array");
  }

  size_t idx, max;
  yyjson_val *ts;
  yyjson_arr_foreach(timeseries, idx, max, ts) {
    MetForecastPoint point;

    point.time = GetJsonString(ts, "time");

    yyjson_val *data = yyjson_obj_get(ts, "data");
    if (data) {
      yyjson_val *instant = yyjson_obj_get(data, "instant");
      if (instant) {
        yyjson_val *details = yyjson_obj_get(instant, "details");
        if (details) {
          point.air_temperature =
              GetJsonDouble(details, "air_temperature", NAN);
          point.relative_humidity =
              GetJsonDouble(details, "relative_humidity", NAN);
          point.wind_speed = GetJsonDouble(details, "wind_speed", NAN);
          point.wind_from_direction =
              GetJsonDouble(details, "wind_from_direction", NAN);
          point.wind_speed_of_gust =
              GetJsonDouble(details, "wind_speed_of_gust", NAN);
          point.air_pressure_at_sea_level =
              GetJsonDouble(details, "air_pressure_at_sea_level", NAN);
          point.cloud_area_fraction =
              GetJsonDouble(details, "cloud_area_fraction", NAN);
        }
      }

      // Get precipitation from next_1_hours if available
      yyjson_val *next_1h = yyjson_obj_get(data, "next_1_hours");
      if (next_1h) {
        yyjson_val *n1h_details = yyjson_obj_get(next_1h, "details");
        if (n1h_details) {
          point.precipitation_amount =
              GetJsonDouble(n1h_details, "precipitation_amount", NAN);
        }
      }
    }

    points.push_back(point);
  }

  yyjson_doc_free(doc);
  return points;
}

// ============================================================
// Bind Function
// ============================================================

static unique_ptr<FunctionData>
MetForecastBind(ClientContext &context, TableFunctionBindInput &input,
                vector<LogicalType> &return_types, vector<string> &names) {
  auto bind_data = make_uniq<MetForecastBindData>();

  // Get parameters
  if (input.inputs.size() < 2) {
    throw InvalidInputException(
        "met_forecast requires latitude and longitude parameters");
  }

  bind_data->latitude = input.inputs[0].GetValue<double>();
  bind_data->longitude = input.inputs[1].GetValue<double>();

  if (input.inputs.size() >= 3 && !input.inputs[2].IsNull()) {
    bind_data->altitude = input.inputs[2].GetValue<double>();
  }

  // Get User-Agent from settings or use default
  Value user_agent_val;
  if (context.TryGetCurrentSetting(MET_USER_AGENT_KEY, user_agent_val)) {
    bind_data->user_agent = user_agent_val.ToString();
  } else {
    bind_data->user_agent = DEFAULT_USER_AGENT;
  }

  // Define output columns
  names = {"time",
           "latitude",
           "longitude",
           "temperature_celsius",
           "humidity_percentage",
           "wind_speed_ms",
           "wind_direction_deg",
           "wind_gust_ms",
           "pressure_hpa",
           "cloud_cover_percentage",
           "precipitation_mm"};

  return_types = {LogicalType::TIMESTAMP_TZ, // time
                  LogicalType::DOUBLE,       // latitude
                  LogicalType::DOUBLE,       // longitude
                  LogicalType::DOUBLE,       // temperature_celsius
                  LogicalType::DOUBLE,       // humidity_percentage
                  LogicalType::DOUBLE,       // wind_speed_ms
                  LogicalType::DOUBLE,       // wind_direction_deg
                  LogicalType::DOUBLE,       // wind_gust_ms
                  LogicalType::DOUBLE,       // pressure_hpa
                  LogicalType::DOUBLE,       // cloud_cover_percentage
                  LogicalType::DOUBLE};      // precipitation_mm

  bind_data->column_names = names;
  bind_data->column_types = return_types;

  return std::move(bind_data);
}

// ============================================================
// Init Global
// ============================================================

static unique_ptr<GlobalTableFunctionState>
MetForecastInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
  auto &bind_data = input.bind_data->Cast<MetForecastBindData>();
  auto state = make_uniq<MetForecastGlobalState>();

  state->latitude = bind_data.latitude;
  state->longitude = bind_data.longitude;

  // Build URL
  string url = "https://api.met.no/weatherapi/locationforecast/2.0/compact?";
  url += "lat=" + StringUtil::Format("%.6f", bind_data.latitude);
  url += "&lon=" + StringUtil::Format("%.6f", bind_data.longitude);
  if (bind_data.altitude >= 0) {
    url += "&altitude=" + StringUtil::Format("%.0f", bind_data.altitude);
  }

  // Make HTTP request with custom User-Agent
  auto &http_util = HTTPUtil::Get(*context.db);
  auto params = http_util.InitializeParameters(context, url);

  HTTPHeaders headers;
  headers["User-Agent"] = bind_data.user_agent;
  GetRequestInfo get_request(url, headers, *params, nullptr, nullptr);

  auto response = http_util.Request(get_request);

  if (!response->Success()) {
    throw IOException("MET API request failed with status %d for URL: %s",
                      static_cast<int32_t>(response->status), url);
  }

  // Parse JSON response
  state->data_points = ParseMetJson(response->body);

  return std::move(state);
}

// ============================================================
// Scan Function
// ============================================================

static void MetForecastScan(ClientContext &context, TableFunctionInput &data,
                            DataChunk &output) {
  auto &state = data.global_state->Cast<MetForecastGlobalState>();

  if (state.finished) {
    output.SetCardinality(0);
    return;
  }

  idx_t count = 0;
  idx_t max_count = STANDARD_VECTOR_SIZE;

  while (count < max_count && state.current_idx < state.data_points.size()) {
    auto &point = state.data_points[state.current_idx];

    // Parse timestamp (ISO 8601 format: 2026-01-20T12:00:00Z)
    timestamp_t ts;
    if (!point.time.empty()) {
      ts = Timestamp::FromString(point.time, true); // true = use_offset for TZ
    }

    output.SetValue(0, count, Value::TIMESTAMPTZ(timestamp_tz_t(ts)));
    output.SetValue(1, count, Value(state.latitude));
    output.SetValue(2, count, Value(state.longitude));
    output.SetValue(3, count,
                    std::isnan(point.air_temperature)
                        ? Value()
                        : Value(point.air_temperature));
    output.SetValue(4, count,
                    std::isnan(point.relative_humidity)
                        ? Value()
                        : Value(point.relative_humidity));
    output.SetValue(5, count,
                    std::isnan(point.wind_speed) ? Value()
                                                 : Value(point.wind_speed));
    output.SetValue(6, count,
                    std::isnan(point.wind_from_direction)
                        ? Value()
                        : Value(point.wind_from_direction));
    output.SetValue(7, count,
                    std::isnan(point.wind_speed_of_gust)
                        ? Value()
                        : Value(point.wind_speed_of_gust));
    output.SetValue(8, count,
                    std::isnan(point.air_pressure_at_sea_level)
                        ? Value()
                        : Value(point.air_pressure_at_sea_level));
    output.SetValue(9, count,
                    std::isnan(point.cloud_area_fraction)
                        ? Value()
                        : Value(point.cloud_area_fraction));
    output.SetValue(10, count,
                    std::isnan(point.precipitation_amount)
                        ? Value()
                        : Value(point.precipitation_amount));

    state.current_idx++;
    count++;
  }

  if (state.current_idx >= state.data_points.size()) {
    state.finished = true;
  }

  output.SetCardinality(count);
}

// ============================================================
// Register Function
// ============================================================

void RegisterMetForecastFunction(ExtensionLoader &loader) {
  auto &db = loader.GetDatabaseInstance();

  // Register configuration option for User-Agent
  auto &config = DBConfig::GetConfig(db);
  config.AddExtensionOption(
      MET_USER_AGENT_KEY,
      "User-Agent header for MET Norway API requests (api.met.no)",
      LogicalType::VARCHAR, Value(DEFAULT_USER_AGENT));

  // Create table function
  TableFunction func("met_forecast",
                     {LogicalType::DOUBLE, LogicalType::DOUBLE}, // lat, lon
                     MetForecastScan, MetForecastBind, MetForecastInitGlobal);

  // Add optional altitude parameter
  func.named_parameters["altitude"] = LogicalType::DOUBLE;

  loader.RegisterFunction(func);
}

} // namespace duckdb
