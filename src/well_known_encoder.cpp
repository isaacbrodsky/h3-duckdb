
#include "well_known_encoder.hpp"
#include "h3api.h"
#include "duckdb/common/string_util.hpp"
#include <cassert>

namespace duckdb {

void WkbEncoder::StartLineString() {
  // little endian + linestring
  buffer = std::string("\x01\x02\x00\x00\x00", 5);
}

void WkbEncoder::Point(double lng, double lat) {
  points.push_back(make_pair(lng, lat));
}

void WkbEncoder::EndLineString() {
  uint32_t numLineStringPoints = points.size();
  assert(numLineStringPoints == points.size());
  assert(sizeof(numLineStringPoints) == 4);
  // TODO: Is this actually little endian?
  buffer.append((char *)&numLineStringPoints, 4);

  for (const auto &vec : points) {
    buffer.append((char *)&vec.first, 8);
    buffer.append((char *)&vec.second, 8);
  }
  points.clear();
}

void WkbEncoder::StartPolygon() {
  // little endian + polygon + 1 linestring inside
  buffer = std::string("\x01\x03\x00\x00\x00\x01\x00\x00\x00", 9);
}

void WkbEncoder::EndPolygon() { EndLineString(); }

void WkbEncoder::StartMultiPolygon(uint32_t polygonCount) {
  // little endian + multipolygon + how many polygons inside
  buffer = std::string("\x01\x07\x00\x00\x00", 5);
  buffer.append((char *)&polygonCount, 4);
}

void WkbEncoder::StartMultiPolygonPolygon(uint32_t loopCount) {
  // little endian + polygon + number of loops
  buffer.append("\x01\x03\x00\x00\x00", 5);
  buffer.append((char *)&loopCount, 4);
}

void WkbEncoder::StartMultiPolygonLoop() { /* no-op */
}

void WkbEncoder::MultiPolygonEmpty() { /* no-op */
}

void WkbEncoder::EndMultiPolygonLoop() { EndLineString(); }

void WkbEncoder::EndMultiPolygonPolygon() { /* no-op */
}

void WkbEncoder::EndMultiPolygon() { /* no-op */
}

std::string WkbEncoder::Finish() { return buffer; }

// *** WKT ***

void WktEncoder::StartLineString() { buffer = "LINESTRING ("; }

void WktEncoder::Point(double lng, double lat) {
  auto sep = firstPoint ? "" : ", ";
  buffer += StringUtil::Format("%s%f %f", sep, lng, lat);
  firstPoint = false;
}

void WktEncoder::EndLineString() { buffer += ")"; }

void WktEncoder::StartPolygon() { buffer = "POLYGON (("; }

void WktEncoder::EndPolygon() { buffer += "))"; }

void WktEncoder::StartMultiPolygon(uint32_t polygonCount) {
  buffer = "MULTIPOLYGON ";
}

void WktEncoder::StartMultiPolygonPolygon(uint32_t loopCount) {
  if (!firstPolygon) {
    buffer += ", ";
  } else {
    buffer += "(";
  }

  buffer += "(";
  firstPolygon = false;
}

void WktEncoder::StartMultiPolygonLoop() {
  if (!firstLoop) {
    buffer += ", ";
  }

  buffer += "(";
  firstLoop = false;
}

void WktEncoder::MultiPolygonEmpty() { buffer += "EMPTY"; }

void WktEncoder::EndMultiPolygonLoop() {
  buffer += ")";
  firstPoint = true;
}

void WktEncoder::EndMultiPolygonPolygon() {
  buffer += ")";
  firstLoop = true;
}

void WktEncoder::EndMultiPolygon() { buffer += ")"; }

std::string WktEncoder::Finish() { return buffer; }

} // namespace duckdb
