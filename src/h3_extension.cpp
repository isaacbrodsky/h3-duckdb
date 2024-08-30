#define DUCKDB_EXTENSION_MAIN
#include "h3_extension.hpp"

#include "duckdb/main/extension_util.hpp"
#include "h3_functions.hpp"
#include "h3api.h"

namespace duckdb {

void H3Extension::Load(DuckDB &db) {
  std::string description =
      StringUtil::Format("H3 hierarchical hexagonal indexing system for "
                         "geospatial data, v%d.%d.%d",
                         H3_VERSION_MAJOR, H3_VERSION_MINOR, H3_VERSION_PATCH);
  ExtensionUtil::RegisterExtension(*db.instance, "h3", {description});

  Connection con(db);
  con.BeginTransaction();

  auto &catalog = Catalog::GetSystemCatalog(*con.context);
  for (auto &fun : H3Functions::GetFunctions()) {
    catalog.CreateFunction(*con.context, fun);
  }

  con.Commit();
}

std::string H3Extension::Name() { return "h3"; }

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void h3_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::H3Extension>();
}

DUCKDB_EXTENSION_API const char *h3_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
