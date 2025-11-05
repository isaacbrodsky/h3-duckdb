
#include "well_known.hpp"
#include "h3api.h"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void WkbEncoder::StartLineString() {
  // little endian + linestring
  buffer = std::string("\x01\x02\x00\x00\x00", 5);
}

void WkbEncoder::Point(double lng, double lat) {
  lineStringPoints.push_back(make_pair(lng, lat));
}

void WkbEncoder::EndLineString() {
  uint32_t numLineStringPoints = lineStringPoints.size();
  assert(numLineStringPoints == lineStringPoints.size());
  assert(sizeof(numLineStringPoints) == 4);
  // TODO: Is this actually little endian?
  buffer.append((char *)&numLineStringPoints, 4);

  for (const auto &vec : lineStringPoints) {
    buffer.append((char *)&vec.first, 8);
    buffer.append((char *)&vec.second, 8);
  }
}

std::string WkbEncoder::Finish() { return buffer; }

void WktEncoder::StartLineString() { buffer = "LINESTRING ("; }

void WktEncoder::Point(double lng, double lat) {
  auto sep = first ? "" : ", ";
  buffer += StringUtil::Format("%s%f %f", sep, lng, lat);
  first = false;
}

void WktEncoder::EndLineString() { buffer += ")"; }

std::string WktEncoder::Finish() { return buffer; }

} // namespace duckdb
