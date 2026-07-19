cmake_minimum_required(VERSION 3.5...3.29)
option(DUCKDB_WASM_EXTENSION "Whether compiling for Wasm target" OFF)

###
# Configuration
###
if(NOT DEFINED EXTENSION_NAME)
    message(FATAL_ERROR "DuckDB extension name is required")
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MAJOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MAJOR=${TARGET_DUCKDB_VERSION_MAJOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_MINOR)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MINOR=${TARGET_DUCKDB_VERSION_MINOR})
endif()
if (DEFINED TARGET_DUCKDB_VERSION_PATCH)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_PATCH=${TARGET_DUCKDB_VERSION_PATCH})
endif()

add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})

if (DEFINED DUCKDB_EXTENSION_API_VERSION_UNSTABLE)
    add_definitions(-DDUCKDB_EXTENSION_API_VERSION_UNSTABLE=${DUCKDB_EXTENSION_API_VERSION_UNSTABLE})
endif()

###
# Build
###
project(${EXTENSION_NAME} LANGUAGES C CXX)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_C_STANDARD_REQUIRED 1)
set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED 1)
set(CMAKE_CXX_STANDARD 20)

# Avoid building tooling we won't need for release
set(BUILD_BENCHMARKS
    OFF
    CACHE BOOL "" FORCE)
set(BUILD_FILTERS
    OFF
    CACHE BOOL "" FORCE)
set(BUILD_GENERATORS
    OFF
    CACHE BOOL "" FORCE)
set(BUILD_TESTING
    OFF
    CACHE BOOL "" FORCE)
set(BUILD_FUZZERS
    OFF
    CACHE BOOL "" FORCE)
set(ENABLE_DOCS
    OFF
    CACHE BOOL "" FORCE)
set(ENABLE_TESTING
    OFF
    CACHE BOOL "" FORCE)
set(ENABLE_LINTING
    OFF
    CACHE BOOL "" FORCE)
set(ENABLE_FORMAT
    OFF
    CACHE BOOL "" FORCE)

# Hide symbols
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_C_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
# Build the core library as static TODO: Is this needed? Consider restoring
# correctly
set(BUILD_SHARED_LIBS OFF)
add_subdirectory(h3)
if(WIN32)
    set_target_properties(h3 PROPERTIES ARCHIVE_OUTPUT_NAME h3_static)
endif()

# Create Extension library
set(EXTENSION_SOURCES
            src/h3_extension.cpp
            src/h3_common.cpp
            src/h3_indexing.cpp
            src/h3_inspection.cpp
            src/h3_hierarchy.cpp
            src/h3_traversal.cpp
            src/h3_vertex.cpp
            src/h3_directededge.cpp
            src/h3_misc.cpp
            src/h3_regions.cpp
            src/well_known_decoder.cpp
            src/well_known_encoder.cpp)
set(LIB_HEADER_FILES src/include/h3_common.hpp src/include/h3_functions.hpp
                     src/include/h3_extension.hpp
                     src/include/well_known_decoder.hpp
                     src/include/well_known_encoder.hpp)
set(ALL_SOURCE_FILES ${EXTENSION_SOURCES} ${LIB_HEADER_FILES})

set(EXTENSION_TARGET_NAME "${EXTENSION_NAME}_ext")
if (DUCKDB_WASM_EXTENSION)
	add_library(${EXTENSION_TARGET_NAME} STATIC ${EXTENSION_SOURCES})
else()
	add_library(${EXTENSION_TARGET_NAME} SHARED ${EXTENSION_SOURCES})
endif()

set_target_properties(${EXTENSION_TARGET_NAME} PROPERTIES OUTPUT_NAME ${EXTENSION_NAME})

# Include own headers
target_include_directories(${EXTENSION_TARGET_NAME} PRIVATE src/include)

# Include DuckDB C API headers
target_include_directories(${EXTENSION_TARGET_NAME} PRIVATE duckdb_capi)

target_include_directories(${EXTENSION_TARGET_NAME} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/h3/src/h3lib/include")

target_link_libraries(${EXTENSION_TARGET_NAME} h3)
