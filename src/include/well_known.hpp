
#pragma once

#include <string>
#include <vector>

namespace duckdb {

class WkbEncoder {
private:
  std::string buffer;
  std::vector<std::pair<double, double>> lineStringPoints;

public:
  WkbEncoder() : buffer(), lineStringPoints() {}
  void StartLineString();
  void Point(double lng, double lat);
  void EndLineString();
  std::string Finish();
};

class WktEncoder {
private:
  std::string buffer;
  bool first;

public:
  WktEncoder() : buffer(), first(true){};
  void StartLineString();
  void Point(double lng, double lat);
  void EndLineString();
  std::string Finish();
};

} // namespace duckdb
