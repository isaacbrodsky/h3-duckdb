cmake_minimum_required(VERSION 2.8.12)
set(TARGET_NAME h3ext)
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
set(BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(BUILD_FILTERS OFF CACHE BOOL "" FORCE)
set(BUILD_GENERATORS OFF CACHE BOOL "" FORCE)
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
set(BUILD_FUZZERS OFF CACHE BOOL "" FORCE)
set(ENABLE_DOCS OFF CACHE BOOL "" FORCE)
set(ENABLE_TESTING OFF CACHE BOOL "" FORCE)

# Build the core library as static
# TODO: Is this needed? Consider restoring correctly
set(BUILD_SHARED_LIBS OFF)
add_subdirectory(h3)

# Build the rest (other than the core library dependency) as shared
set(BUILD_SHARED_LIBS ON)


set(EXTENSION_SOURCES
  h3-extension.cpp
  h3_common.cpp
  h3_functions/h3_indexing.cpp
  h3_functions/h3_inspection.cpp
  h3_functions/h3_hierarchy.cpp
  h3_functions/h3_traversal.cpp
  h3_functions/h3_vertex.cpp
  h3_functions/h3_directededge.cpp
  h3_functions/h3_misc.cpp
  h3_functions/h3_regions.cpp
)
set(LIB_HEADER_FILES
  src/include/h3_common.hpp
  src/include/h3_functions.hpp
  src/include/h3ext_extension.hpp
)
set(ALL_SOURCE_FILES ${EXTENSION_SOURCES} ${LIB_HEADER_FILES})

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

# Note this must be the INSTALL target name.
# See https://stackoverflow.com/a/71080574
target_link_libraries(${EXTENSION_NAME} h3Targets::libh3)

include_directories(src/include)
include_directories("${CMAKE_CURRENT_BINARY_DIR}/h3/src/h3lib/include")

build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
target_link_libraries(${LOADABLE_EXTENSION_NAME} h3)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")


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
    add_dependencies(${EXTENSION_NAME} format-ext)
elseif(NOT CLANG_FORMAT_PATH)
    message(WARNING "clang-format was not detected, "
                    "so automatic source code reformatting is disabled")
endif()
