#include "h3_common.hpp"

#include <sstream>

namespace h3duckdb {

std::string ToHexString(H3Index index) {
  // TODO: Optimize this
  std::stringstream ss;
  ss << std::hex << index;
  return ss.str();
}

void ThrowH3Error(H3Error err) {
  if (err) {
    throw H3Exception(std::string("H3 error: ") + std::to_string(err));
  }
}

} // namespace h3duckdb
