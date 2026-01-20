#include "grib_function.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "grib2_ffi.h"

namespace duckdb {

// Helper to check if path is an HTTP URL
static bool IsHttpUrl(const string &path) {
  return StringUtil::StartsWith(path, "http://") ||
         StringUtil::StartsWith(path, "https://");
}

// ENUM type names
static const char *DISCIPLINE_ENUM = "grib_discipline";
static const char *SURFACE_ENUM = "grib_surface";
static const char *PARAMETER_ENUM = "grib_parameter";

// High cardinality for LIMIT pushdown detection
static constexpr idx_t GRIB_REPORTED_CARDINALITY = 100000000;

// Discipline enum values
static const vector<string> DISCIPLINE_VALUES = {
    "Meteorological", "Hydrological",  "Land_Surface", "Satellite",
    "Space_Weather",  "Oceanographic", "Unknown"};

// Surface type enum values
static const vector<string> SURFACE_VALUES = {"Ground_Water",
                                              "Cloud_Base",
                                              "Cloud_Top",
                                              "Isotherm_0C",
                                              "Adiabatic_Condensation",
                                              "Max_Wind",
                                              "Tropopause",
                                              "Top_of_Atmosphere",
                                              "Sea_Bottom",
                                              "Entire_Atmosphere",
                                              "Isothermal",
                                              "Isobaric",
                                              "Mean_Sea_Level",
                                              "Altitude_MSL",
                                              "Height_Above_Ground",
                                              "Sigma",
                                              "Hybrid",
                                              "Depth_Below_Land",
                                              "Isentropic",
                                              "Pressure_From_Ground",
                                              "Potential_Vorticity",
                                              "Eta",
                                              "Mixed_Layer_Depth",
                                              "Depth_Below_Sea",
                                              "Entire_Atmos_Layer",
                                              "PBL",
                                              "Unknown"};

// Parameter enum values
static const vector<string> PARAMETER_VALUES = {"Temperature",
                                                "Virtual_Temp",
                                                "Potential_Temp",
                                                "Max_Temp",
                                                "Min_Temp",
                                                "Dew_Point",
                                                "Dew_Point_Depression",
                                                "Lapse_Rate",
                                                "Apparent_Temp",
                                                "Skin_Temp",
                                                "Specific_Humidity",
                                                "Relative_Humidity",
                                                "Mixing_Ratio",
                                                "Precipitable_Water",
                                                "Precip_Rate",
                                                "Total_Precip",
                                                "Snow_Depth",
                                                "Snow_Cover",
                                                "Wind_Direction",
                                                "Wind_Speed",
                                                "U_Wind",
                                                "V_Wind",
                                                "Vertical_Velocity",
                                                "Vorticity",
                                                "Divergence",
                                                "Wind_Gust",
                                                "Pressure",
                                                "Pressure_MSL",
                                                "Geopotential",
                                                "Geopotential_Height",
                                                "Density",
                                                "PBL_Height",
                                                "SW_Radiation",
                                                "LW_Radiation",
                                                "Cloud_Cover",
                                                "Low_Cloud",
                                                "Mid_Cloud",
                                                "High_Cloud",
                                                "Visibility",
                                                "CAPE",
                                                "CIN",
                                                "Lifted_Index",
                                                "Helicity",
                                                "Reflectivity",
                                                "Soil_Temp",
                                                "Soil_Moisture",
                                                "Wave_Height",
                                                "Sea_Temp",
                                                "Unknown"};

// Bind data - stores file paths and ENUM types
struct GribBindData : public TableFunctionData {
  vector<string> file_paths; // Support multiple paths

  // ENUM type handles
  LogicalType discipline_type;
  LogicalType surface_type;
  LogicalType parameter_type;
};

// Map discipline code to enum index
static uint8_t DisciplineToEnumIndex(uint8_t code) {
  switch (code) {
  case 0:
    return 0; // Meteorological
  case 1:
    return 1; // Hydrological
  case 2:
    return 2; // Land_Surface
  case 3:
    return 3; // Satellite
  case 4:
    return 4; // Space_Weather
  case 10:
    return 5; // Oceanographic
  default:
    return 6; // Unknown
  }
}

// Map surface code to enum index
static uint8_t SurfaceToEnumIndex(uint8_t code) {
  switch (code) {
  case 1:
    return 0; // Ground_Water
  case 2:
    return 1; // Cloud_Base
  case 3:
    return 2; // Cloud_Top
  case 4:
    return 3; // Isotherm_0C
  case 5:
    return 4; // Adiabatic_Condensation
  case 6:
    return 5; // Max_Wind
  case 7:
    return 6; // Tropopause
  case 8:
    return 7; // Top_of_Atmosphere
  case 9:
    return 8; // Sea_Bottom
  case 10:
    return 9; // Entire_Atmosphere
  case 20:
    return 10; // Isothermal
  case 100:
    return 11; // Isobaric
  case 101:
    return 12; // Mean_Sea_Level
  case 102:
    return 13; // Altitude_MSL
  case 103:
    return 14; // Height_Above_Ground
  case 104:
    return 15; // Sigma
  case 105:
    return 16; // Hybrid
  case 106:
    return 17; // Depth_Below_Land
  case 107:
    return 18; // Isentropic
  case 108:
    return 19; // Pressure_From_Ground
  case 109:
    return 20; // Potential_Vorticity
  case 111:
    return 21; // Eta
  case 117:
    return 22; // Mixed_Layer_Depth
  case 160:
    return 23; // Depth_Below_Sea
  case 200:
    return 24; // Entire_Atmos_Layer
  case 220:
    return 25; // PBL
  default:
    return 26; // Unknown
  }
}

// Map parameter (disc, cat, num) to enum index
static uint8_t ParameterToEnumIndex(uint8_t disc, uint8_t cat, uint8_t num) {
  if (disc == 0) {
    if (cat == 0) {
      switch (num) {
      case 0:
        return 0; // Temperature
      case 1:
        return 1; // Virtual_Temp
      case 2:
        return 2; // Potential_Temp
      case 4:
        return 3; // Max_Temp
      case 5:
        return 4; // Min_Temp
      case 6:
        return 5; // Dew_Point
      case 7:
        return 6; // Dew_Point_Depression
      case 8:
        return 7; // Lapse_Rate
      case 15:
      case 21:
        return 8; // Apparent_Temp
      case 17:
        return 9; // Skin_Temp
      }
    } else if (cat == 1) {
      switch (num) {
      case 0:
        return 10; // Specific_Humidity
      case 1:
        return 11; // Relative_Humidity
      case 2:
        return 12; // Mixing_Ratio
      case 3:
        return 13; // Precipitable_Water
      case 7:
        return 14; // Precip_Rate
      case 8:
        return 15; // Total_Precip
      case 11:
        return 16; // Snow_Depth
      case 60:
        return 17; // Snow_Cover
      }
    } else if (cat == 2) {
      switch (num) {
      case 0:
        return 18; // Wind_Direction
      case 1:
        return 19; // Wind_Speed
      case 2:
        return 20; // U_Wind
      case 3:
        return 21; // V_Wind
      case 8:
      case 9:
        return 22; // Vertical_Velocity
      case 10:
      case 12:
        return 23; // Vorticity
      case 11:
      case 13:
        return 24; // Divergence
      case 22:
        return 25; // Wind_Gust
      }
    } else if (cat == 3) {
      switch (num) {
      case 0:
        return 26; // Pressure
      case 1:
        return 27; // Pressure_MSL
      case 4:
        return 28; // Geopotential
      case 5:
        return 29; // Geopotential_Height
      case 10:
        return 30; // Density
      case 196:
        return 31; // PBL_Height
      }
    } else if (cat == 4) {
      return 32; // SW_Radiation
    } else if (cat == 5) {
      return 33; // LW_Radiation
    } else if (cat == 6) {
      switch (num) {
      case 1:
        return 34; // Cloud_Cover
      case 3:
        return 35; // Low_Cloud
      case 4:
        return 36; // Mid_Cloud
      case 5:
        return 37; // High_Cloud
      }
    } else if (cat == 7) {
      switch (num) {
      case 6:
        return 39; // CAPE
      case 7:
        return 40; // CIN
      case 0:
      case 1:
        return 41; // Lifted_Index
      case 8:
        return 42; // Helicity
      }
    } else if (cat == 16) {
      return 43; // Reflectivity
    } else if (cat == 19) {
      if (num == 0)
        return 38; // Visibility
    }
  } else if (disc == 2 && cat == 0) {
    if (num == 2)
      return 44; // Soil_Temp
    if (num == 3 || num == 22)
      return 45; // Soil_Moisture
  } else if (disc == 10) {
    if (cat == 0 && num == 3)
      return 46; // Wave_Height
    if (cat == 3 && num == 0)
      return 47; // Sea_Temp
  }
  return 48; // Unknown
}

// Helper to create ENUM types
static void CreateEnumTypes(GribBindData &bind_data) {
  Vector disc_vec(LogicalType::VARCHAR, DISCIPLINE_VALUES.size());
  auto disc_data = FlatVector::GetData<string_t>(disc_vec);
  for (idx_t i = 0; i < DISCIPLINE_VALUES.size(); i++) {
    disc_data[i] = StringVector::AddString(disc_vec, DISCIPLINE_VALUES[i]);
  }
  bind_data.discipline_type =
      LogicalType::ENUM(DISCIPLINE_ENUM, disc_vec, DISCIPLINE_VALUES.size());

  Vector surf_vec(LogicalType::VARCHAR, SURFACE_VALUES.size());
  auto surf_data = FlatVector::GetData<string_t>(surf_vec);
  for (idx_t i = 0; i < SURFACE_VALUES.size(); i++) {
    surf_data[i] = StringVector::AddString(surf_vec, SURFACE_VALUES[i]);
  }
  bind_data.surface_type =
      LogicalType::ENUM(SURFACE_ENUM, surf_vec, SURFACE_VALUES.size());

  Vector param_vec(LogicalType::VARCHAR, PARAMETER_VALUES.size());
  auto param_data = FlatVector::GetData<string_t>(param_vec);
  for (idx_t i = 0; i < PARAMETER_VALUES.size(); i++) {
    param_data[i] = StringVector::AddString(param_vec, PARAMETER_VALUES[i]);
  }
  bind_data.parameter_type =
      LogicalType::ENUM(PARAMETER_ENUM, param_vec, PARAMETER_VALUES.size());
}

// Helper to open a GRIB file/URL
static Grib2Reader *OpenGribSource(ClientContext &context, const string &path,
                                   string &http_data_out) {
  char *error = nullptr;
  Grib2Reader *reader = nullptr;

  if (IsHttpUrl(path)) {
    auto &http_util = HTTPUtil::Get(*context.db);
    auto params = http_util.InitializeParameters(context, path);

    HTTPHeaders headers;
    GetRequestInfo get_request(path, headers, *params, nullptr, nullptr);

    auto response = http_util.Request(get_request);

    if (!response->Success()) {
      throw IOException("HTTP request failed with status " +
                        to_string(static_cast<int32_t>(response->status)) +
                        " for URL: " + path);
    }

    http_data_out = response->body;

    reader = grib2_open_from_bytes(
        reinterpret_cast<const uint8_t *>(http_data_out.data()),
        http_data_out.size(), &error);
  } else {
    reader = grib2_open_with_error(path.c_str(), &error);
  }

  if (!reader) {
    string error_msg = error ? error : "Unknown error";
    if (error)
      grib2_free_error(error);
    throw IOException("Failed to open GRIB source: " + error_msg);
  }

  return reader;
}

// ============================================================================
// Standard table function (for literal paths and arrays)
// ============================================================================

struct GribGlobalState : public GlobalTableFunctionState {
  Grib2Reader *reader = nullptr;
  idx_t current_file_idx = 0;
  idx_t total_points = 0;
  idx_t rows_returned = 0;
  idx_t limit_from_query = 0;
  bool finished = false;
  string http_data;
  ClientContext *context_ptr = nullptr;

  ~GribGlobalState() {
    if (reader) {
      grib2_close(reader);
    }
  }

  idx_t MaxThreads() const override { return 1; }

  bool OpenFile(const string &path) {
    if (reader) {
      grib2_close(reader);
      reader = nullptr;
    }
    http_data.clear();
    reader = OpenGribSource(*context_ptr, path, http_data);
    total_points += grib2_total_points(reader);
    return true;
  }
};

struct GribLocalState : public LocalTableFunctionState {};

// Bind function - accepts VARCHAR or LIST(VARCHAR)
static unique_ptr<FunctionData> GribBind(ClientContext &context,
                                         TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types,
                                         vector<string> &names) {
  auto bind_data = make_uniq<GribBindData>();

  if (input.inputs.empty()) {
    throw InvalidInputException(
        "read_grib() requires a file path or array of paths");
  }

  auto &arg = input.inputs[0];
  auto arg_type = arg.type().id();

  if (arg_type == LogicalTypeId::VARCHAR) {
    bind_data->file_paths.push_back(arg.GetValue<string>());
  } else if (arg_type == LogicalTypeId::LIST) {
    auto &list_children = ListValue::GetChildren(arg);
    if (list_children.empty()) {
      throw InvalidInputException("read_grib() array cannot be empty");
    }
    for (auto &child : list_children) {
      bind_data->file_paths.push_back(child.GetValue<string>());
    }
  } else {
    throw InvalidInputException(
        "read_grib() requires VARCHAR or VARCHAR[] argument");
  }

  CreateEnumTypes(*bind_data);

  names = {"latitude",      "longitude", "value",         "discipline",
           "surface",       "parameter", "forecast_time", "surface_value",
           "message_index", "file_index"};

  return_types = {LogicalType::DOUBLE,     LogicalType::DOUBLE,
                  LogicalType::DOUBLE,     bind_data->discipline_type,
                  bind_data->surface_type, bind_data->parameter_type,
                  LogicalType::BIGINT,     LogicalType::DOUBLE,
                  LogicalType::UINTEGER,   LogicalType::UINTEGER};

  return std::move(bind_data);
}

static unique_ptr<NodeStatistics>
GribCardinality(ClientContext &context, const FunctionData *bind_data) {
  return make_uniq<NodeStatistics>(GRIB_REPORTED_CARDINALITY,
                                   GRIB_REPORTED_CARDINALITY);
}

static unique_ptr<GlobalTableFunctionState>
GribInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
  auto state = make_uniq<GribGlobalState>();
  auto &bind_data = input.bind_data->Cast<GribBindData>();

  state->context_ptr = &context;

  if (input.op) {
    idx_t estimated = input.op->estimated_cardinality;
    if (estimated > 0 && estimated < GRIB_REPORTED_CARDINALITY) {
      state->limit_from_query = estimated;
    }
  }

  if (!bind_data.file_paths.empty()) {
    state->OpenFile(bind_data.file_paths[0]);
  }

  return std::move(state);
}

static unique_ptr<LocalTableFunctionState>
GribInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
              GlobalTableFunctionState *global_state) {
  return make_uniq<GribLocalState>();
}

static void GribScan(ClientContext &context, TableFunctionInput &data,
                     DataChunk &output) {
  auto &state = data.global_state->Cast<GribGlobalState>();
  auto &bind_data = data.bind_data->Cast<GribBindData>();

  if (state.finished) {
    output.SetCardinality(0);
    return;
  }

  idx_t effective_limit = GRIB_REPORTED_CARDINALITY;
  if (state.limit_from_query > 0) {
    effective_limit = state.limit_from_query;
  }

  if (state.rows_returned >= effective_limit) {
    state.finished = true;
    output.SetCardinality(0);
    return;
  }

  idx_t remaining = effective_limit - state.rows_returned;
  idx_t batch_size = std::min(remaining, (idx_t)STANDARD_VECTOR_SIZE);

  Grib2Batch batch = grib2_read_batch(state.reader, batch_size);

  if (batch.error) {
    string error_msg = batch.error;
    grib2_free_batch(batch);
    throw IOException("Error reading GRIB data: " + error_msg);
  }

  while (batch.count == 0) {
    grib2_free_batch(batch);
    state.current_file_idx++;
    if (state.current_file_idx >= bind_data.file_paths.size()) {
      state.finished = true;
      output.SetCardinality(0);
      return;
    }
    state.OpenFile(bind_data.file_paths[state.current_file_idx]);
    batch = grib2_read_batch(state.reader, batch_size);
    if (batch.error) {
      string error_msg = batch.error;
      grib2_free_batch(batch);
      throw IOException("Error reading GRIB data: " + error_msg);
    }
  }

  idx_t current_file = state.current_file_idx;
  for (idx_t i = 0; i < batch.count; i++) {
    auto &point = batch.data[i];
    output.SetValue(0, i, Value::DOUBLE(point.latitude));
    output.SetValue(1, i, Value::DOUBLE(point.longitude));
    output.SetValue(2, i, Value::DOUBLE(point.value));

    auto disc_idx = DisciplineToEnumIndex(point.discipline);
    output.SetValue(3, i,
                    Value(DISCIPLINE_VALUES[disc_idx])
                        .DefaultCastAs(bind_data.discipline_type));

    auto surf_idx = SurfaceToEnumIndex(point.surface_type);
    output.SetValue(
        4, i,
        Value(SURFACE_VALUES[surf_idx]).DefaultCastAs(bind_data.surface_type));

    auto param_idx = ParameterToEnumIndex(
        point.discipline, point.parameter_category, point.parameter_number);
    output.SetValue(5, i,
                    Value(PARAMETER_VALUES[param_idx])
                        .DefaultCastAs(bind_data.parameter_type));

    output.SetValue(6, i, Value::BIGINT(point.forecast_time));
    output.SetValue(7, i, Value::DOUBLE(point.surface_value));
    output.SetValue(8, i, Value::UINTEGER(point.message_index));
    output.SetValue(9, i, Value::UINTEGER(static_cast<uint32_t>(current_file)));
  }

  output.SetCardinality(batch.count);
  state.rows_returned += batch.count;
  grib2_free_batch(batch);
}

static double GribProgress(ClientContext &context,
                           const FunctionData *bind_data_p,
                           const GlobalTableFunctionState *gstate_p) {
  auto &state = gstate_p->Cast<GribGlobalState>();
  auto &bind_data = bind_data_p->Cast<GribBindData>();

  if (state.total_points == 0)
    return -1.0;
  double file_progress = static_cast<double>(state.current_file_idx) /
                         static_cast<double>(bind_data.file_paths.size());
  return file_progress * 100.0;
}

// ============================================================================
// In-out table function (for LATERAL joins)
// ============================================================================

struct GribInOutGlobalState : public GlobalTableFunctionState {
  idx_t MaxThreads() const override { return 1; }
};

struct GribInOutLocalState : public LocalTableFunctionState {
  Grib2Reader *reader = nullptr;
  string http_data;
  bool initialized = false;
  ClientContext *context_ptr = nullptr;

  ~GribInOutLocalState() {
    if (reader) {
      grib2_close(reader);
    }
  }

  void Reset() {
    if (reader) {
      grib2_close(reader);
      reader = nullptr;
    }
    http_data.clear();
    initialized = false;
  }
};

static unique_ptr<FunctionData> GribInOutBind(ClientContext &context,
                                              TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types,
                                              vector<string> &names) {
  auto bind_data = make_uniq<GribBindData>();
  CreateEnumTypes(*bind_data);

  // Output columns (no file_index needed - LATERAL handles row correlation)
  names = {"latitude",      "longitude",     "value",
           "discipline",    "surface",       "parameter",
           "forecast_time", "surface_value", "message_index"};

  return_types = {LogicalType::DOUBLE,     LogicalType::DOUBLE,
                  LogicalType::DOUBLE,     bind_data->discipline_type,
                  bind_data->surface_type, bind_data->parameter_type,
                  LogicalType::BIGINT,     LogicalType::DOUBLE,
                  LogicalType::UINTEGER};

  return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState>
GribInOutInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
  return make_uniq<GribInOutGlobalState>();
}

static unique_ptr<LocalTableFunctionState>
GribInOutInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                   GlobalTableFunctionState *global_state) {
  auto state = make_uniq<GribInOutLocalState>();
  state->context_ptr = &context.client;
  return std::move(state);
}

static OperatorResultType GribInOutFunction(ExecutionContext &context,
                                            TableFunctionInput &data,
                                            DataChunk &input,
                                            DataChunk &output) {
  auto &bind_data = data.bind_data->Cast<GribBindData>();
  auto &lstate = data.local_state->Cast<GribInOutLocalState>();

  // Initialize reader from input if not done
  if (!lstate.initialized) {
    // Get URL/path from input (first column)
    auto &input_vector = input.data[0];
    if (input.size() == 0) {
      return OperatorResultType::NEED_MORE_INPUT;
    }

    // Handle NULL input
    if (FlatVector::IsNull(input_vector, 0)) {
      lstate.initialized = true;
      output.SetCardinality(0);
      return OperatorResultType::NEED_MORE_INPUT;
    }

    auto path = FlatVector::GetData<string_t>(input_vector)[0].GetString();

    try {
      lstate.reader =
          OpenGribSource(*lstate.context_ptr, path, lstate.http_data);
    } catch (Exception &e) {
      throw IOException("Failed to open GRIB source in LATERAL: " +
                        string(e.what()));
    }
    lstate.initialized = true;
  }

  // Read batch
  Grib2Batch batch = grib2_read_batch(lstate.reader, STANDARD_VECTOR_SIZE);

  if (batch.error) {
    string error_msg = batch.error;
    grib2_free_batch(batch);
    throw IOException("Error reading GRIB data: " + error_msg);
  }

  if (batch.count == 0) {
    grib2_free_batch(batch);
    lstate.Reset();
    output.SetCardinality(0);
    return OperatorResultType::NEED_MORE_INPUT;
  }

  // Fill output
  for (idx_t i = 0; i < batch.count; i++) {
    auto &point = batch.data[i];
    output.SetValue(0, i, Value::DOUBLE(point.latitude));
    output.SetValue(1, i, Value::DOUBLE(point.longitude));
    output.SetValue(2, i, Value::DOUBLE(point.value));

    auto disc_idx = DisciplineToEnumIndex(point.discipline);
    output.SetValue(3, i,
                    Value(DISCIPLINE_VALUES[disc_idx])
                        .DefaultCastAs(bind_data.discipline_type));

    auto surf_idx = SurfaceToEnumIndex(point.surface_type);
    output.SetValue(
        4, i,
        Value(SURFACE_VALUES[surf_idx]).DefaultCastAs(bind_data.surface_type));

    auto param_idx = ParameterToEnumIndex(
        point.discipline, point.parameter_category, point.parameter_number);
    output.SetValue(5, i,
                    Value(PARAMETER_VALUES[param_idx])
                        .DefaultCastAs(bind_data.parameter_type));

    output.SetValue(6, i, Value::BIGINT(point.forecast_time));
    output.SetValue(7, i, Value::DOUBLE(point.surface_value));
    output.SetValue(8, i, Value::UINTEGER(point.message_index));
  }

  output.SetCardinality(batch.count);
  bool has_more = batch.has_more;
  grib2_free_batch(batch);

  return has_more ? OperatorResultType::HAVE_MORE_OUTPUT
                  : OperatorResultType::NEED_MORE_INPUT;
}

// ============================================================================
// Registration
// ============================================================================

void RegisterGribFunction(ExtensionLoader &loader) {
  // Standard table function with VARCHAR
  TableFunction grib_func("read_grib", {LogicalType::VARCHAR}, GribScan,
                          GribBind, GribInitGlobal);
  grib_func.init_local = GribInitLocal;
  grib_func.cardinality = GribCardinality;
  grib_func.table_scan_progress = GribProgress;

  // Standard table function with LIST(VARCHAR)
  TableFunction grib_func_array("read_grib",
                                {LogicalType::LIST(LogicalType::VARCHAR)},
                                GribScan, GribBind, GribInitGlobal);
  grib_func_array.init_local = GribInitLocal;
  grib_func_array.cardinality = GribCardinality;
  grib_func_array.table_scan_progress = GribProgress;

  // In-out function for LATERAL joins: read_grib_lateral(path)
  // Use when path comes from another table: SELECT * FROM urls, LATERAL
  // read_grib_lateral(urls.path)
  TableFunction grib_inout("read_grib_lateral", {LogicalType::VARCHAR}, nullptr,
                           GribInOutBind, GribInOutInitGlobal,
                           GribInOutInitLocal);
  grib_inout.in_out_function = GribInOutFunction;

  loader.RegisterFunction(grib_func);
  loader.RegisterFunction(grib_func_array);
  loader.RegisterFunction(grib_inout);
}

void RegisterGribEnumTypes(DatabaseInstance &db) {
  Connection conn(db);

  conn.Query("CREATE TYPE IF NOT EXISTS " + string(DISCIPLINE_ENUM) +
             " AS ENUM ("
             "'Meteorological', 'Hydrological', 'Land_Surface', 'Satellite', "
             "'Space_Weather', 'Oceanographic', 'Unknown')");

  conn.Query(
      "CREATE TYPE IF NOT EXISTS " + string(SURFACE_ENUM) +
      " AS ENUM ("
      "'Ground_Water', 'Cloud_Base', 'Cloud_Top', 'Isotherm_0C', "
      "'Adiabatic_Condensation', 'Max_Wind', 'Tropopause', "
      "'Top_of_Atmosphere', "
      "'Sea_Bottom', 'Entire_Atmosphere', 'Isothermal', 'Isobaric', "
      "'Mean_Sea_Level', 'Altitude_MSL', 'Height_Above_Ground', 'Sigma', "
      "'Hybrid', 'Depth_Below_Land', 'Isentropic', 'Pressure_From_Ground', "
      "'Potential_Vorticity', 'Eta', 'Mixed_Layer_Depth', 'Depth_Below_Sea', "
      "'Entire_Atmos_Layer', 'PBL', 'Unknown')");

  conn.Query(
      "CREATE TYPE IF NOT EXISTS " + string(PARAMETER_ENUM) +
      " AS ENUM ("
      "'Temperature', 'Virtual_Temp', 'Potential_Temp', 'Max_Temp', "
      "'Min_Temp', "
      "'Dew_Point', 'Dew_Point_Depression', 'Lapse_Rate', 'Apparent_Temp', "
      "'Skin_Temp', "
      "'Specific_Humidity', 'Relative_Humidity', 'Mixing_Ratio', "
      "'Precipitable_Water', "
      "'Precip_Rate', 'Total_Precip', 'Snow_Depth', 'Snow_Cover', "
      "'Wind_Direction', 'Wind_Speed', 'U_Wind', 'V_Wind', "
      "'Vertical_Velocity', "
      "'Vorticity', 'Divergence', 'Wind_Gust', "
      "'Pressure', 'Pressure_MSL', 'Geopotential', 'Geopotential_Height', "
      "'Density', 'PBL_Height', 'SW_Radiation', 'LW_Radiation', "
      "'Cloud_Cover', 'Low_Cloud', 'Mid_Cloud', 'High_Cloud', "
      "'Visibility', 'CAPE', 'CIN', 'Lifted_Index', 'Helicity', "
      "'Reflectivity', "
      "'Soil_Temp', 'Soil_Moisture', 'Wave_Height', 'Sea_Temp', 'Unknown')");
}

} // namespace duckdb
