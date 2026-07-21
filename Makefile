.PHONY: clean clean_all format-check format ubsan coverage

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Main extension configuration
EXTENSION_NAME=h3

# Set to 1 to enable Unstable API (binaries will only work on TARGET_DUCKDB_VERSION, forwards compatibility will be broken)
# WARNING: When set to 1, the duckdb_extension.h from the TARGET_DUCKDB_VERSION must be used, using any other version of
#          the header is unsafe.
USE_UNSTABLE_C_API=0

# The DuckDB version to target
TARGET_DUCKDB_VERSION=v1.2.0

all: configure release

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/c_cpp.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_cmake
clean_all: clean clean_configure

format-check:
	python3 duckdb/scripts/format.py --all --check --directories src test

format:
	python3 duckdb/scripts/format.py --all --fix --noconfirm --directories src test

ubsan: export EXTRA_CMAKE_FLAGS=-DENABLE_UBSAN=1
ubsan: export UBSAN_OPTIONS=print_stacktrace=1:halt_on_error=1
ubsan: debug test_debug

coverage:
	export EXTRA_CMAKE_FLAGS=-DENABLE_COVERAGE=1
	cmake --build cmake_build/debug --config Debug --target clean-coverage
	make debug test_debug
	cmake --build cmake_build/debug --config Debug --target coverage
