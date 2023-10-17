#define DUCKDB_EXTENSION_MAIN
#define DUCKDB_BUILD_LOADABLE_EXTENSION
#include "h3-extension.hpp"

#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "h3_functions.hpp"

namespace duckdb {

void H3Extension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	for (auto &fun : H3Functions::GetFunctions()) {
		catalog.CreateFunction(*con.context, fun);
	}

	con.Commit();
}

std::string H3Extension::Name() {
	return "h3ext";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void h3ext_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::H3Extension>();
}

DUCKDB_EXTENSION_API const char *h3ext_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
