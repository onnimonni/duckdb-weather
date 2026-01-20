cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME weather)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Rust library configuration
set(RUST_LIB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rust)
set(RUST_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR}/rust-target)

# Determine Rust target and library name based on platform
if(APPLE)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
        set(RUST_TARGET "aarch64-apple-darwin")
    else()
        set(RUST_TARGET "x86_64-apple-darwin")
    endif()
    set(RUST_LIB_NAME "libgrib2_ffi.a")
elseif(WIN32)
    set(RUST_TARGET "x86_64-pc-windows-msvc")
    set(RUST_LIB_NAME "grib2_ffi.lib")
else()
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
        set(RUST_TARGET "aarch64-unknown-linux-gnu")
    else()
        set(RUST_TARGET "x86_64-unknown-linux-gnu")
    endif()
    set(RUST_LIB_NAME "libgrib2_ffi.a")
endif()

set(RUST_LIB_PATH ${RUST_TARGET_DIR}/${RUST_TARGET}/release/${RUST_LIB_NAME})

# Custom command to build Rust library
add_custom_command(
    OUTPUT ${RUST_LIB_PATH}
    COMMAND cargo build --release --target ${RUST_TARGET} --target-dir ${RUST_TARGET_DIR}
    WORKING_DIRECTORY ${RUST_LIB_DIR}
    COMMENT "Building Rust GRIB2 FFI library"
    DEPENDS ${RUST_LIB_DIR}/src/lib.rs ${RUST_LIB_DIR}/Cargo.toml
)

add_custom_target(rust_grib2_ffi DEPENDS ${RUST_LIB_PATH})

# C++ extension sources
set(EXTENSION_SOURCES
    src/weather_extension.cpp
    src/grib_function.cpp
    src/gfs_forecast_function.cpp
    src/weather_function.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Add dependency on Rust library
add_dependencies(${EXTENSION_NAME} rust_grib2_ffi)
add_dependencies(${LOADABLE_EXTENSION_NAME} rust_grib2_ffi)

# Link Rust library
target_link_libraries(${EXTENSION_NAME} ${RUST_LIB_PATH})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${RUST_LIB_PATH})

# macOS requires additional frameworks for Rust
if(APPLE)
    target_link_libraries(${EXTENSION_NAME} "-framework Security" "-framework CoreFoundation")
    target_link_libraries(${LOADABLE_EXTENSION_NAME} "-framework Security" "-framework CoreFoundation")
endif()

# Linux may need pthread and dl
if(UNIX AND NOT APPLE)
    target_link_libraries(${EXTENSION_NAME} pthread dl)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} pthread dl)
endif()

install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
