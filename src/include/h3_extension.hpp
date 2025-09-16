//===----------------------------------------------------------------------===//
//                         DuckDB
//
// h3_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/extension.hpp"

namespace duckdb {

class H3Extension : public Extension {
public:
  void Load(ExtensionLoader &loader) override;
  std::string Name() override;
};

} // namespace duckdb
