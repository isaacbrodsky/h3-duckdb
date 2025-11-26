
#pragma once

#include <h3api.h>
#include "duckdb/common/string_util.hpp"

namespace duckdb {

void DecodeWkbPolygon(
    string_t input, GeoPolygon &polygon,
    duckdb::shared_ptr<std::vector<LatLng>> &outerVerts,
    std::vector<GeoLoop> &holes,
    std::vector<duckdb::shared_ptr<std::vector<LatLng>>> &holesVerts);

} // namespace duckdb
