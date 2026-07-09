//===----------------------------------------------------------------------===//
//                         DuckDB
//
// h3_common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "h3api.h"

#define DUCKDB_API_NO_DEPRECATED
#include "duckdb_extension.h"

DUCKDB_EXTENSION_EXTERN

#include <string>

namespace h3duckdb {

std::string ToHexString(H3Index index);

void ThrowH3Error(H3Error err);

class H3Exception : public std::runtime_error {
public:
  H3Exception(std::string err) : std::runtime_error(err){};
};

} // namespace h3duckdb
