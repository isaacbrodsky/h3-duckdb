# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(h3
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LINKED_LIBS "h3/lib/libh3.a"
    LOAD_TESTS
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
