#include "h3_extension.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "h3_functions.hpp"
#include "h3api.h"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
  std::string description =
      StringUtil::Format("H3 hierarchical hexagonal indexing system for "
                         "geospatial data, v%d.%d.%d",
                         H3_VERSION_MAJOR, H3_VERSION_MINOR, H3_VERSION_PATCH);
  loader.SetDescription(description);

  for (auto &fun : H3Functions::GetFunctions()) {
    loader.RegisterFunction(fun);
  }
}

void H3Extension::Load(ExtensionLoader &loader) { LoadInternal(loader); }

std::string H3Extension::Name() { return "h3"; }

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(h3, loader) { duckdb::LoadInternal(loader); }
}
