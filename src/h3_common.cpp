#include "h3_common.hpp"

#include <sstream>

namespace h3duckdb {

std::string ToHexString(H3Index index) {
  // TODO: Optimize this
  std::stringstream ss;
  ss << std::hex << index;
  return ss.str();
}

std::string DuckdbToString(duckdb_string_t *str) {
  uint32_t len = duckdb_string_t_length(*str);
  const char *data = duckdb_string_t_data(str);
  return std::string(data, len);
}

} // namespace h3duckdb
