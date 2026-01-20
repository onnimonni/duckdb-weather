PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=weather
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Build Rust library first
rust:
	cd rust && cargo build --release

clean-rust:
	cd rust && cargo clean

clean-all: clean clean-rust
