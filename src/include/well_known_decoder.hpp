
#pragma once

#include "h3_common.hpp"
#include <vector>
#include <memory>

namespace h3duckdb {

void DecodeWkbPolygon(
    const std::string &input, GeoPolygon &polygon,
    std::shared_ptr<std::vector<LatLng>> &outerVerts,
    std::vector<GeoLoop> &holes,
    std::vector<std::shared_ptr<std::vector<LatLng>>> &holesVerts);

void DecodeWktPolygon(
    const std::string &input, GeoPolygon &polygon,
    std::shared_ptr<std::vector<LatLng>> &outerVerts,
    std::vector<GeoLoop> &holes,
    std::vector<std::shared_ptr<std::vector<LatLng>>> &holesVerts);

} // namespace h3duckdb
