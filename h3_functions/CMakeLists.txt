add_library_unity(
  duckdb_h3_functions
  OBJECT
  h3_cell_to_parent.cpp
  h3_valid.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_h3_functions>
    PARENT_SCOPE)
