
#pragma once

#include "h3_common.hpp"
#include <vector>

 namespace h3duckdb {

 void DecodeWkbPolygon(
    const std::string &input, GeoPolygon &polygon,
    std::vector<LatLng> &outerVerts,
    std::vector<GeoLoop> &holes,
    std::vector<std::vector<LatLng>> &holesVerts);

 void DecodeWktPolygon(
    const std::string &input, GeoPolygon &polygon,
    std::vector<LatLng> &outerVerts,
    std::vector<GeoLoop> &holes,
    std::vector<std::vector<LatLng>> &holesVerts);

} // namespace h3duckdb
