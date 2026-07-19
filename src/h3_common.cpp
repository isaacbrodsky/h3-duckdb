#include "h3_common.hpp"

#include <sstream>
#include <charconv>

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

void AppendDouble(std::string &str, double d) {
  char buf[100] = {0};
  // TODO: Switch away from fixed format to minimize generated WKT
  auto res =
      std::to_chars(buf, buf + sizeof(buf), d, std::chars_format::fixed, 6);
  // TODO: Check res.ec
  str.append(buf, res.ptr);
}

} // namespace h3duckdb
