#include "h3_extension.hpp"

#include "h3_functions.hpp"
#include "h3api.h"

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection,
                            duckdb_extension_info info,
                            struct duckdb_extension_access *access) {
  // TODO: Set extension description
  //  std::string description =
  //      StringUtil::Format("H3 hierarchical hexagonal indexing system for "
  //                         "geospatial data, v%d.%d.%d",
  //                         H3_VERSION_MAJOR, H3_VERSION_MINOR,
  //                         H3_VERSION_PATCH);
  //  loader.SetDescription(description);
  // loader.SetDescription("Lua embedded scripting language, " LUA_RELEASE);
  // TODO: Set extension version

  for (auto &fun : h3duckdb::H3Functions::GetFunctions()) {
    duckdb_register_scalar_function(connection, fun);
    duckdb_destroy_scalar_function(&fun);
  }

  // Return true to indicate succesful initialization
  return true;
}
