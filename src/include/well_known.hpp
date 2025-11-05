
#pragma once

#include <string>
#include <vector>

namespace duckdb {

class WkbEncoder {
private:
  std::string buffer;
  std::vector<std::pair<double, double>> points;

public:
  WkbEncoder() : buffer(), points() {}
  void Point(double lng, double lat);
  void StartLineString();
  void EndLineString();
  void StartPolygon();
  void EndPolygon();
  std::string Finish();
};

class WktEncoder {
private:
  std::string buffer;
  bool first;

public:
  WktEncoder() : buffer(), first(true){};
  void Point(double lng, double lat);
  void StartLineString();
  void EndLineString();
  void StartPolygon();
  void EndPolygon();
  std::string Finish();
};

} // namespace duckdb
