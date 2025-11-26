
#include "well_known_decoder.hpp"
#include "h3_functions.hpp"

namespace duckdb {

template <typename T> static T ReadWkb(string_t input, size_t &inputIdx) {
  if (inputIdx + sizeof(T) > input.GetSize()) {
    throw InvalidInputException(StringUtil::Format(
        "Invalid WKB: failed to read %lu bytes at %lu", sizeof(T), inputIdx));
  }

  T *resultPtr = (T *)((uint8_t *)input.GetData() + inputIdx);
  inputIdx += sizeof(T);
  return *resultPtr;
}

void DecodeWkbGeoLoop(string_t input, size_t &inputIdx,
                      duckdb::shared_ptr<std::vector<LatLng>> &verts,
                      GeoLoop &loop) {
  uint32_t numVerts = ReadWkb<uint32_t>(input, inputIdx);

  for (uint32_t vertIdx = 0; vertIdx < numVerts; vertIdx++) {
    double lng = ReadWkb<double>(input, inputIdx);
    double lat = ReadWkb<double>(input, inputIdx);

    LatLng ll = {.lat = degsToRads(lat), .lng = degsToRads(lng)};

    verts->push_back(ll);
  }

  loop.numVerts = numVerts;
  loop.verts = verts->data();
}

void DecodeWkbPolygon(
    string_t input, GeoPolygon &polygon,
    duckdb::shared_ptr<std::vector<LatLng>> &outerVerts,
    std::vector<GeoLoop> &holes,
    std::vector<duckdb::shared_ptr<std::vector<LatLng>>> &holesVerts) {
  size_t strIndex = 0;

  uint8_t orderMark = ReadWkb<uint8_t>(input, strIndex);

  if (orderMark != 0x01) {
    throw InvalidInputException(StringUtil::Format(
        "Invalid WKB: expected little endian at %lu", strIndex));
  }

  uint32_t type = ReadWkb<uint32_t>(input, strIndex);

  if (type == 0) {
    return; // EMPTY
  }
  if (type != 3) {
    throw InvalidInputException(
        StringUtil::Format("Invalid WKB: expected polygon at %lu", strIndex));
  }

  uint32_t loopCount = ReadWkb<uint32_t>(input, strIndex);
  if (loopCount == 0) {
    return; // Empty
  }

  DecodeWkbGeoLoop(input, strIndex, outerVerts, polygon.geoloop);

  if (loopCount > 1) {
    for (uint32_t loopIdx = 1; loopIdx < loopCount; loopIdx++) {
      GeoLoop hole;
      auto verts = duckdb::make_shared_ptr<std::vector<LatLng>>();
      DecodeWkbGeoLoop(input, strIndex, verts, hole);
      holes.push_back(hole);
      holesVerts.push_back(verts);
    }

    polygon.numHoles = holes.size();
    polygon.holes = holes.data();
  }
}

// *** WKT ***

static const std::string POLYGON = "POLYGON";
static const std::string EMPTY = "EMPTY";

static size_t WktWhitespace(const std::string &str, size_t offset) {
  while (str[offset] == ' ') {
    offset++;
  }
  return offset;
}

static size_t ReadWktNumber(const std::string &str, size_t offset,
                            double &num) {
  size_t start = offset;
  while (str[offset] != ' ' && str[offset] != ')' && str[offset] != ',') {
    offset++;
  }
  std::string part = str.substr(start, offset - start);

  try {
    num = std::stod(part);
    return offset;
  } catch (std::invalid_argument const &ex) {
    throw InvalidInputException(
        StringUtil::Format("Invalid number around %lu, %lu", start, offset));
  }
}

static size_t ReadWktGeoLoop(const std::string &str, size_t offset,
                             duckdb::shared_ptr<std::vector<LatLng>> verts,
                             GeoLoop &loop) {
  if (str[offset] != '(') {
    throw InvalidInputException(
        StringUtil::Format("Expected ( at pos %lu", offset));
  }

  offset++;
  offset = WktWhitespace(str, offset);

  while (str[offset] != ')') {
    double x, y;
    offset = ReadWktNumber(str, offset, x);
    offset = WktWhitespace(str, offset);
    offset = ReadWktNumber(str, offset, y);
    offset = WktWhitespace(str, offset);
    verts->push_back({.lat = degsToRads(y), .lng = degsToRads(x)});

    if (str[offset] == ',') {
      offset++;
      offset = WktWhitespace(str, offset);
    }
  }
  // Consume the )
  offset++;

  loop.numVerts = verts->size();
  loop.verts = verts->data();

  offset = WktWhitespace(str, offset);
  return offset;
}

void DecodeWktPolygon(
    std::string str, GeoPolygon &polygon,
    duckdb::shared_ptr<std::vector<LatLng>> &outerVerts,
    std::vector<GeoLoop> &holes,
    std::vector<duckdb::shared_ptr<std::vector<LatLng>>> &holesVerts) {
  if (str.rfind(POLYGON, 0) != 0) {
    return;
  }

  size_t strIndex = POLYGON.length();
  strIndex = WktWhitespace(str, strIndex);

  if (str.rfind(EMPTY, strIndex) == strIndex) {
    return;
  }

  if (str[strIndex] == '(') {
    strIndex++;
    strIndex = WktWhitespace(str, strIndex);

    strIndex = ReadWktGeoLoop(str, strIndex, outerVerts, polygon.geoloop);

    while (strIndex < str.length() && str[strIndex] == ',') {
      strIndex++;
      strIndex = WktWhitespace(str, strIndex);
      if (str[strIndex] == '(') {
        GeoLoop hole;
        auto verts = duckdb::make_shared_ptr<std::vector<LatLng>>();
        strIndex = ReadWktGeoLoop(str, strIndex, verts, hole);
        holes.push_back(hole);
        holesVerts.push_back(verts);
      } else {
        throw InvalidInputException(StringUtil::Format(
            "Invalid WKT: expected a hole loop '(' after ',' at pos %lu",
            strIndex));
      }
    }
    if (str[strIndex] != ')') {
      throw InvalidInputException(StringUtil::Format(
          "Invalid WKT: expected a hole loop ',' or final ')' at pos %lu",
          strIndex));
    }

    polygon.numHoles = holes.size();
    polygon.holes = holes.data();
  }
}

} // namespace duckdb
