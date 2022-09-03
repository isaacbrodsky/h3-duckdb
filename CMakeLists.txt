cmake_minimum_required(VERSION 2.8.12)

if(POLICY CMP0026)
  cmake_policy(SET CMP0026 NEW)
endif()

if(POLICY CMP0015)
  cmake_policy(SET CMP0015 NEW)
endif()

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

if(POLICY CMP0051)
  cmake_policy(SET CMP0051 NEW)
endif()

if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()

if(POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif()

if(CMAKE_VERSION VERSION_LESS "3.1")
  add_compile_options(-std=c++11)
else()
  set(CMAKE_CXX_STANDARD 11)
endif()

set(TARGET_NAME h3ext)

project(${TARGET_NAME})

set(CMAKE_VERBOSE_MAKEFILE OFF)

option(
  OSX_BUILD_UNIVERSAL
  "Build both architectures on OSX and create a single binary containing both."
  FALSE)
if(OSX_BUILD_UNIVERSAL)
  if(NOT APPLE)
    error("This only makes sense on OSX")
  endif()
  set(CMAKE_OSX_ARCHITECTURES
      "x86_64;arm64"
      CACHE STRING "Build architectures for Mac OS X" FORCE)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_C_STANDARD_REQUIRED 1)
set(CMAKE_C_STANDARD 11)
if(WIN32)
  set(CMAKE_CXX_STANDARD_REQUIRED 1)
  set(CMAKE_CXX_STANDARD 20)
endif()

# Avoid building tooling we won't need for release
set(BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(BUILD_FILTERS OFF CACHE BOOL "" FORCE)
set(BUILD_GENERATORS OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(BUILD_FUZZERS OFF CACHE BOOL "" FORCE)
set(ENABLE_DOCS OFF CACHE BOOL "" FORCE)
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)

# Build the core library as static
set(BUILD_SHARED_LIBS OFF)
add_subdirectory(h3)

# Build the rest (other than the core library dependency) as shared
set(BUILD_SHARED_LIBS ON)

include_directories(${DUCKDB_INCLUDE_FOLDER})
include_directories(include)

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  add_compile_options(-Wall -Werror -pedantic -fsanitize=address)
endif()

link_directories(${DUCKDB_LIBRARY_FOLDER})

set(LIB_SOURCE_FILES h3-extension.cpp h3_common.cpp h3_functions/h3_indexing.cpp h3_functions/h3_inspection.cpp h3_functions/h3_hierarchy.cpp)
set(LIB_HEADER_FILES include/h3_common.hpp include/h3_functions.hpp include/h3-extension.hpp)
set(ALL_SOURCE_FILES ${LIB_SOURCE_FILES})

add_library(${TARGET_NAME} SHARED ${LIB_SOURCE_FILES})
set_target_properties(${TARGET_NAME} PROPERTIES PREFIX "")
target_link_libraries(${TARGET_NAME} h3)

# Automatic code formatting
# Give preference to clang-format-9
find_program(CLANG_FORMAT_PATH NAMES clang-format-9 clang-format)
cmake_dependent_option(
    ENABLE_FORMAT "Enable running clang-format before compiling" ON
    "CLANG_FORMAT_PATH" OFF)
if(ENABLE_FORMAT)
    # Format
    add_custom_target(format-ext
        COMMAND ${CLANG_FORMAT_PATH}
        -style=file:${CMAKE_CURRENT_SOURCE_DIR}/duckdb/.clang-format
        -i
        ${ALL_SOURCE_FILES}
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMENT "Formatting sources"
        )
    # Always do formatting
    add_dependencies(${TARGET_NAME} format-ext)
elseif(NOT CLANG_FORMAT_PATH)
    message(WARNING "clang-format was not detected, "
                    "so automatic source code reformatting is disabled")
endif()

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  target_link_libraries(${TARGET_NAME} -fsanitize=address)
endif()

set_target_properties(${TARGET_NAME} PROPERTIES OUTPUT_NAME "h3")
set_target_properties(${TARGET_NAME} PROPERTIES SUFFIX ".duckdb_extension")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS
                                                  "-undefined dynamic_lookup")
endif()

if(WIN32)
  target_link_libraries(${TARGET_NAME} duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
endif()

if(MSVC)
  set_target_properties(
    ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
                              "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
  set_target_properties(
    ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
                              "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
endif()

if(WIN32)
  set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS_DEBUG
                                                  "/DELAYLOAD:duckdb.dll")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG
      "${CMAKE_EXE_LINKER_FLAGS_DEBUG}  /DELAYLOAD:duckdb.dll")
  set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS_RELEASE
                                                  "/DELAYLOAD:duckdb.dll")
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE
      "${CMAKE_EXE_LINKER_FLAGS_RELEASE}  /DELAYLOAD:duckdb.dll")
  # This is only strictly required in non-Visual-Studio builds like Ninja:
  target_link_libraries(${TARGET_NAME} delayimp)
endif()
