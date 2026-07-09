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

template <typename T> inline H3Index IndexFromVector(T *data, idx_t idx) {
  static_assert(std::is_same<T, duckdb_string_t>::value ||
                    std::is_same<T, uint64_t>::value ||
                    std::is_same<T, int64_t>::value,
                "T must be an acceptable type");
  constexpr auto IsStringT = std::is_same<T, duckdb_string_t>::value;
  H3Index cell;
  if (IsStringT) {
    auto str = (duckdb_string_t *)&data[idx];
    H3Error err = stringToH3(duckdb_string_t_data(str), &cell);
    if (err) {
      cell = 0;
    }
  } else {
    cell = ((uint64_t *)data)[idx];
  }
  return cell;
}

void ThrowH3Error(H3Error err);

class H3Exception : public std::runtime_error {
public:
  H3Exception(std::string err) : std::runtime_error(err){};
};

} // namespace h3duckdb
