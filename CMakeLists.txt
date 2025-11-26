cmake_minimum_required(VERSION 3.5...3.29)
set(TARGET_NAME h3)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_C_STANDARD_REQUIRED 1)
set(CMAKE_C_STANDARD 11)
if(WIN32)
  set(CMAKE_CXX_STANDARD_REQUIRED 1)
  set(CMAKE_CXX_STANDARD 20)
endif()

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

# Build the core library as static TODO: Is this needed? Consider restoring
# correctly
set(BUILD_SHARED_LIBS OFF)
add_subdirectory(h3)

# Build the rest (other than the core library dependency) as shared
set(BUILD_SHARED_LIBS ON)

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
    src/well_known.cpp
    src/well_known_decoder.cpp)
set(LIB_HEADER_FILES src/include/h3_common.hpp src/include/h3_functions.hpp
                     src/include/h3_extension.hpp src/include/well_known.hpp
                     src/include/well_known_decoder.hpp)
set(ALL_SOURCE_FILES ${EXTENSION_SOURCES} ${LIB_HEADER_FILES})

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

# Note this must be the INSTALL target name. See
# https://stackoverflow.com/a/71080574
target_link_libraries(${EXTENSION_NAME} h3)

include_directories(src/include)
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/h3/src/h3lib/include")

build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} h3)

install(
  TARGETS ${EXTENSION_NAME} h3
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
