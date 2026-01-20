//! GRIB2 FFI library for DuckDB weather extension
//!
//! Provides C-compatible functions for streaming GRIB2 files.
//! Supports both file paths and in-memory byte arrays.

use grib::Grib2SubmessageDecoder;
use std::ffi::{c_char, c_double, c_uint, CStr, CString};
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Seek};
use std::ptr;

/// A single data point from a GRIB2 file
#[repr(C)]
pub struct Grib2DataPoint {
    pub latitude: c_double,
    pub longitude: c_double,
    pub value: c_double,
    pub discipline: u8,
    pub parameter_category: u8,
    pub parameter_number: u8,
    pub forecast_time: i64,
    pub surface_type: u8,
    pub surface_value: c_double,
    pub message_index: c_uint,
}

/// Batch of data points for efficient transfer
#[repr(C)]
pub struct Grib2Batch {
    pub data: *mut Grib2DataPoint,
    pub count: usize,
    pub has_more: bool,
    pub error: *mut c_char,
}

/// Opaque reader handle for streaming
pub struct Grib2Reader {
    messages: Vec<ParsedMessage>,
    current_message: usize,
    current_point: usize,
}

struct ParsedMessage {
    discipline: u8,
    parameter_category: u8,
    parameter_number: u8,
    forecast_time: i64,
    surface_type: u8,
    surface_value: f64,
    message_index: u32,
    points: Vec<(f64, f64, f64)>, // (lat, lon, value)
}

impl Grib2Reader {
    fn from_reader<R: Read + Seek>(reader: R) -> Result<Self, String> {
        let grib2 = grib::from_reader(reader).map_err(|e| format!("Failed to parse GRIB: {}", e))?;

        let mut messages = Vec::new();

        for (msg_idx, submessage) in grib2.iter() {
            let discipline = submessage.indicator().discipline;
            let prod_def = submessage.prod_def();

            let param_cat = prod_def.parameter_category().unwrap_or(0);
            let param_num = prod_def.parameter_number().unwrap_or(0);

            let forecast_time = prod_def
                .forecast_time()
                .map(|ft| ft.value as i64)
                .unwrap_or(0);

            let (surface_type, surface_value) = prod_def
                .fixed_surfaces()
                .map(|(first, _)| (first.surface_type, first.value() as f64))
                .unwrap_or((0, 0.0));

            let latlons = match submessage.latlons() {
                Ok(ll) => ll,
                Err(_) => continue,
            };

            let decoder = match Grib2SubmessageDecoder::from(submessage) {
                Ok(d) => d,
                Err(_) => continue,
            };

            let values = match decoder.dispatch() {
                Ok(v) => v,
                Err(_) => continue,
            };

            let flat_index = (msg_idx.0 * 1000 + msg_idx.1) as u32;

            let points: Vec<(f64, f64, f64)> = latlons
                .zip(values)
                .map(|((lat, lon), value)| {
                    let lon_normalized = if lon > 180.0 { lon - 360.0 } else { lon };
                    (lat as f64, lon_normalized as f64, value as f64)
                })
                .collect();

            messages.push(ParsedMessage {
                discipline,
                parameter_category: param_cat,
                parameter_number: param_num,
                forecast_time,
                surface_type,
                surface_value,
                message_index: flat_index,
                points,
            });
        }

        Ok(Grib2Reader {
            messages,
            current_message: 0,
            current_point: 0,
        })
    }

    /// Open from file path
    fn new(path: &str) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| format!("Failed to open file: {}", e))?;
        let reader = BufReader::new(file);
        Self::from_reader(reader)
    }

    /// Open from in-memory bytes (copies data to owned Vec for Seek support)
    fn from_bytes(data: &[u8]) -> Result<Self, String> {
        let owned_data = data.to_vec();
        let cursor = Cursor::new(owned_data);
        Self::from_reader(cursor)
    }

    fn read_batch(&mut self, max_count: usize) -> Grib2Batch {
        let mut points = Vec::with_capacity(max_count);

        while points.len() < max_count {
            if self.current_message >= self.messages.len() {
                break;
            }

            let msg = &self.messages[self.current_message];

            while self.current_point < msg.points.len() && points.len() < max_count {
                let (lat, lon, value) = msg.points[self.current_point];
                points.push(Grib2DataPoint {
                    latitude: lat,
                    longitude: lon,
                    value,
                    discipline: msg.discipline,
                    parameter_category: msg.parameter_category,
                    parameter_number: msg.parameter_number,
                    forecast_time: msg.forecast_time,
                    surface_type: msg.surface_type,
                    surface_value: msg.surface_value,
                    message_index: msg.message_index,
                });
                self.current_point += 1;
            }

            if self.current_point >= msg.points.len() {
                self.current_message += 1;
                self.current_point = 0;
            }
        }

        let has_more = self.current_message < self.messages.len();
        let count = points.len();

        if count == 0 {
            Grib2Batch {
                data: ptr::null_mut(),
                count: 0,
                has_more: false,
                error: ptr::null_mut(),
            }
        } else {
            let data = points.as_mut_ptr();
            std::mem::forget(points);
            Grib2Batch {
                data,
                count,
                has_more,
                error: ptr::null_mut(),
            }
        }
    }

    fn total_points(&self) -> usize {
        self.messages.iter().map(|m| m.points.len()).sum()
    }
}

// ============ C FFI Functions ============

/// Open a GRIB2 file for streaming reads
/// Returns opaque handle, caller must close with grib2_close
#[no_mangle]
pub extern "C" fn grib2_open(path: *const c_char) -> *mut Grib2Reader {
    let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    match Grib2Reader::new(path_str) {
        Ok(reader) => Box::into_raw(Box::new(reader)),
        Err(_) => ptr::null_mut(),
    }
}

/// Open a GRIB2 file and get error message if failed
#[no_mangle]
pub extern "C" fn grib2_open_with_error(path: *const c_char, error: *mut *mut c_char) -> *mut Grib2Reader {
    let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(e) => {
            unsafe {
                *error = CString::new(format!("Invalid UTF-8 in path: {}", e))
                    .unwrap()
                    .into_raw();
            }
            return ptr::null_mut();
        }
    };

    match Grib2Reader::new(path_str) {
        Ok(reader) => {
            unsafe { *error = ptr::null_mut(); }
            Box::into_raw(Box::new(reader))
        }
        Err(e) => {
            unsafe {
                *error = CString::new(e).unwrap().into_raw();
            }
            ptr::null_mut()
        }
    }
}

/// Open a GRIB2 reader from in-memory bytes (for HTTP fetched data)
/// Returns opaque handle, caller must close with grib2_close
#[no_mangle]
pub extern "C" fn grib2_open_from_bytes(
    data: *const u8,
    len: usize,
    error: *mut *mut c_char
) -> *mut Grib2Reader {
    if data.is_null() || len == 0 {
        unsafe {
            *error = CString::new("Empty or null data").unwrap().into_raw();
        }
        return ptr::null_mut();
    }

    let bytes = unsafe { std::slice::from_raw_parts(data, len) };

    match Grib2Reader::from_bytes(bytes) {
        Ok(reader) => {
            unsafe { *error = ptr::null_mut(); }
            Box::into_raw(Box::new(reader))
        }
        Err(e) => {
            unsafe {
                *error = CString::new(e).unwrap().into_raw();
            }
            ptr::null_mut()
        }
    }
}

/// Read a batch of data points (up to max_count)
/// Caller must free batch with grib2_free_batch
#[no_mangle]
pub extern "C" fn grib2_read_batch(reader: *mut Grib2Reader, max_count: usize) -> Grib2Batch {
    if reader.is_null() {
        return Grib2Batch {
            data: ptr::null_mut(),
            count: 0,
            has_more: false,
            error: CString::new("Null reader").unwrap().into_raw(),
        };
    }

    let reader = unsafe { &mut *reader };
    reader.read_batch(max_count)
}

/// Get total number of data points in file (for cardinality)
#[no_mangle]
pub extern "C" fn grib2_total_points(reader: *mut Grib2Reader) -> usize {
    if reader.is_null() {
        return 0;
    }
    let reader = unsafe { &*reader };
    reader.total_points()
}

/// Close the reader and free resources
#[no_mangle]
pub extern "C" fn grib2_close(reader: *mut Grib2Reader) {
    if !reader.is_null() {
        unsafe {
            let _ = Box::from_raw(reader);
        }
    }
}

/// Free a batch of data points
#[no_mangle]
pub extern "C" fn grib2_free_batch(batch: Grib2Batch) {
    if !batch.data.is_null() {
        unsafe {
            let _ = Vec::from_raw_parts(batch.data, batch.count, batch.count);
        }
    }
    if !batch.error.is_null() {
        unsafe {
            let _ = CString::from_raw(batch.error);
        }
    }
}

/// Free an error string
#[no_mangle]
pub extern "C" fn grib2_free_error(error: *mut c_char) {
    if !error.is_null() {
        unsafe {
            let _ = CString::from_raw(error);
        }
    }
}

// ============ Legacy API (kept for compatibility) ============

/// Result of reading a GRIB2 file (legacy)
#[repr(C)]
pub struct Grib2ReadResult {
    pub data: *mut Grib2DataPoint,
    pub count: usize,
    pub error: *mut c_char,
}

/// Read entire GRIB2 file at once (legacy)
#[no_mangle]
pub extern "C" fn grib2_read_file(path: *const c_char) -> Grib2ReadResult {
    let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            return Grib2ReadResult {
                data: ptr::null_mut(),
                count: 0,
                error: CString::new("Invalid UTF-8 in path").unwrap().into_raw(),
            };
        }
    };

    match Grib2Reader::new(path_str) {
        Ok(mut reader) => {
            let total = reader.total_points();
            let batch = reader.read_batch(total);
            Grib2ReadResult {
                data: batch.data,
                count: batch.count,
                error: batch.error,
            }
        }
        Err(e) => Grib2ReadResult {
            data: ptr::null_mut(),
            count: 0,
            error: CString::new(e).unwrap().into_raw(),
        },
    }
}

/// Free legacy result
#[no_mangle]
pub extern "C" fn grib2_free_result(result: Grib2ReadResult) {
    if !result.data.is_null() {
        unsafe {
            let _ = Vec::from_raw_parts(result.data, result.count, result.count);
        }
    }
    if !result.error.is_null() {
        unsafe {
            let _ = CString::from_raw(result.error);
        }
    }
}
