#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Data point from GRIB2 file
typedef struct {
  double latitude;
  double longitude;
  double value;
  uint8_t discipline;
  uint8_t parameter_category;
  uint8_t parameter_number;
  int64_t forecast_time;
  uint8_t surface_type;
  double surface_value;
  uint32_t message_index;
} Grib2DataPoint;

// Batch of data points for streaming
typedef struct {
  Grib2DataPoint *data;
  size_t count;
  bool has_more;
  char *error;
} Grib2Batch;

// Opaque reader handle
typedef struct Grib2Reader Grib2Reader;

// Streaming API - file path
Grib2Reader *grib2_open(const char *path);
Grib2Reader *grib2_open_with_error(const char *path, char **error);

// Streaming API - in-memory bytes (for HTTP fetched data)
Grib2Reader *grib2_open_from_bytes(const uint8_t *data, size_t len,
                                   char **error);

// Reading and cleanup
Grib2Batch grib2_read_batch(Grib2Reader *reader, size_t max_count);
size_t grib2_total_points(Grib2Reader *reader);
void grib2_close(Grib2Reader *reader);
void grib2_free_batch(Grib2Batch batch);
void grib2_free_error(char *error);

// Legacy API (reads entire file)
typedef struct {
  Grib2DataPoint *data;
  size_t count;
  char *error;
} Grib2ReadResult;

Grib2ReadResult grib2_read_file(const char *path);
void grib2_free_result(Grib2ReadResult result);

#ifdef __cplusplus
}
#endif
