
#pragma once

#include <string>
#include <vector>
#include <cstdint>

namespace duckdb {

class WkbEncoder {
private:
  std::string buffer;
  std::vector<std::pair<double, double>> points;
  std::vector<std::vector<std::pair<double, double>>> multipolygonPoints;

public:
  WkbEncoder() : buffer(), points(), multipolygonPoints() {}
  void Point(double lng, double lat);
  void StartLineString();
  void EndLineString();
  void StartPolygon();
  void EndPolygon();
  void StartMultiPolygon(uint32_t polygonCount);
  void StartMultiPolygonPolygon(uint32_t loopCount);
  void StartMultiPolygonLoop();
  void MultiPolygonEmpty();
  void EndMultiPolygonLoop();
  void EndMultiPolygonPolygon();
  void EndMultiPolygon();
  std::string Finish();
};

class WktEncoder {
private:
  std::string buffer;
  bool firstPoint;
  bool firstLoop;
  bool firstPolygon;

public:
  WktEncoder()
      : buffer(), firstPoint(true), firstLoop(true), firstPolygon(true){};
  void Point(double lng, double lat);
  void StartLineString();
  void EndLineString();
  void StartPolygon();
  void EndPolygon();
  void StartMultiPolygon(uint32_t polygonCount);
  void StartMultiPolygonPolygon(uint32_t loopCount);
  void StartMultiPolygonLoop();
  void MultiPolygonEmpty();
  void EndMultiPolygonLoop();
  void EndMultiPolygonPolygon();
  void EndMultiPolygon();
  std::string Finish();
};

} // namespace duckdb
