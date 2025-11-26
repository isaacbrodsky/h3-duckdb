#include "h3_common.hpp"
#include "h3_functions.hpp"
#include "well_known.hpp"
#include "well_known_decoder.hpp"

#include "duckdb/common/helper.hpp"

namespace duckdb {

static const std::string POLYGON = "POLYGON";
static const std::string EMPTY = "EMPTY";

// TODO: For convenience, 0 is returned instead of throwing. However, this may
// actually be interpreted by cellsToMultiPolygon as the index referring to base
// cell 0.
struct CellsToMultiPolygonInputOperator {
  static H3Index Get(const UnifiedVectorFormat &child_data,
                     const size_t offset) {
    return ((H3Index *)child_data.data)[child_data.sel->get_index(offset)];
  }
};

struct CellsToMultiPolygonVarcharInputOperator {
  static H3Index Get(const UnifiedVectorFormat &child_data,
                     const size_t offset) {
    auto str = ((string_t *)child_data.data)[child_data.sel->get_index(offset)]
                   .GetString();
    H3Index cell;
    H3Error err = stringToH3(str.c_str(), &cell);
    if (err) {
      return 0;
    } else {
      return cell;
    }
  }
};

static uint32_t PolygonCount(const LinkedGeoPolygon *lgp) {
  uint32_t count = 0;
  for (auto polygon = lgp; polygon && polygon->first; polygon = polygon->next) {
    count++;
  }
  return count;
}

static uint32_t LoopCount(const LinkedGeoPolygon *lgp) {
  uint32_t count = 0;
  for (auto loop = lgp->first; loop && loop->first; loop = loop->next) {
    count++;
  }
  return count;
}

template <typename InputType, class InputOperator, class Encoder, bool IsBlob>
static void CellsToMultiPolygonFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  D_ASSERT(args.ColumnCount() == 1);
  auto count = args.size();

  Vector &lhs = args.data[0];
  if (lhs.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(lhs);
    return;
  }

  auto lists_size = ListVector::GetListSize(lhs);
  auto &child_vector = ListVector::GetEntry(lhs);
  child_vector.Flatten(lists_size);

  UnifiedVectorFormat child_data;
  child_vector.ToUnifiedFormat(lists_size, child_data);

  UnifiedVectorFormat lists_data;
  lhs.ToUnifiedFormat(count, lists_data);
  auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_entries = FlatVector::GetData<string_t>(result);
  auto &result_validity = FlatVector::Validity(result);

  idx_t offset = 0;
  for (idx_t i = 0; i < count; i++) {
    auto list_index = lists_data.sel->get_index(i);

    if (!lists_data.validity.RowIsValid(list_index)) {
      result_validity.SetInvalid(i);
      continue;
    }

    vector<H3Index> input_set(list_entries[i].length);
    for (size_t j = 0; j < list_entries[i].length; j++) {
      if (child_data.validity.RowIsValid(
              child_data.sel->get_index(list_entries[i].offset + j))) {
        input_set[j] =
            InputOperator::Get(child_data, list_entries[i].offset + j);
      }
    }
    LinkedGeoPolygon first_lgp;
    H3Error err = cellsToLinkedMultiPolygon(input_set.data(), input_set.size(),
                                            &first_lgp);

    if (err) {
      result_validity.SetInvalid(i);
    } else {
      auto enc = Encoder();
      auto polygon_count = PolygonCount(&first_lgp);
      enc.StartMultiPolygon(polygon_count);

      if (first_lgp.first) {
        LinkedGeoPolygon *lgp = &first_lgp;
        while (lgp) {
          auto loop_count = LoopCount(lgp);
          enc.StartMultiPolygonPolygon(loop_count);
          LinkedGeoLoop *loop = lgp->first;
          while (loop) {
            enc.StartMultiPolygonLoop();
            LinkedLatLng *lat_lng = loop->first;
            while (lat_lng) {
              enc.Point(radsToDegs(lat_lng->vertex.lng),
                        radsToDegs(lat_lng->vertex.lat));
              lat_lng = lat_lng->next;
            }

            if (loop->first) {
              // Duplicate first vertex, to close the polygon
              enc.Point(radsToDegs(loop->first->vertex.lng),
                        radsToDegs(loop->first->vertex.lat));
            }

            loop = loop->next;
            enc.EndMultiPolygonLoop();
          }

          lgp = lgp->next;
          enc.EndMultiPolygonPolygon();
        }

        enc.EndMultiPolygon();
      } else {
        enc.MultiPolygonEmpty();
      }

      auto str = enc.Finish();
      if (IsBlob) {
        auto added_str = StringVector::AddStringOrBlob(result, str);
        result.SetValue(i, Value::BLOB(const_data_ptr_cast(added_str.GetData()),
                                       added_str.GetSize()));
      } else {
        result.SetValue(i, StringVector::AddString(result, str));
      }

      destroyLinkedMultiPolygon(&first_lgp);
    }
  }

  if (lhs.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static size_t whitespace(const std::string &str, size_t offset) {
  while (str[offset] == ' ') {
    offset++;
  }
  return offset;
}

static size_t readNumber(const std::string &str, size_t offset, double &num) {
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

static size_t readGeoLoop(const std::string &str, size_t offset,
                          duckdb::shared_ptr<std::vector<LatLng>> verts,
                          GeoLoop &loop) {
  if (str[offset] != '(') {
    throw InvalidInputException(
        StringUtil::Format("Expected ( at pos %lu", offset));
  }

  offset++;
  offset = whitespace(str, offset);

  while (str[offset] != ')') {
    double x, y;
    offset = readNumber(str, offset, x);
    offset = whitespace(str, offset);
    offset = readNumber(str, offset, y);
    offset = whitespace(str, offset);
    verts->push_back({.lat = degsToRads(y), .lng = degsToRads(x)});

    if (str[offset] == ',') {
      offset++;
      offset = whitespace(str, offset);
    }
  }
  // Consume the )
  offset++;

  loop.numVerts = verts->size();
  loop.verts = verts->data();

  offset = whitespace(str, offset);
  return offset;
}

static void PolygonWktToCellsFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
  // TODO: Note this function is not fully noexcept -- some invalid WKT strings
  // will throw, others will return empty lists.
  BinaryExecutor::Execute<string_t, int, list_entry_t>(
      args.data[0], args.data[1], result, args.size(),
      [&](string_t input, int res) {
        GeoPolygon polygon;
        int32_t flags = 0;

        std::string str = input.GetString();

        uint64_t offset = ListVector::GetListSize(result);
        if (str.rfind(POLYGON, 0) != 0) {
          return list_entry_t(offset, 0);
        }

        size_t strIndex = POLYGON.length();
        strIndex = whitespace(str, strIndex);

        if (str.rfind(EMPTY, strIndex) == strIndex) {
          return list_entry_t(offset, 0);
        }

        if (str[strIndex] == '(') {
          strIndex++;
          strIndex = whitespace(str, strIndex);

          auto outerVerts = duckdb::make_shared_ptr<std::vector<LatLng>>();
          strIndex = readGeoLoop(str, strIndex, outerVerts, polygon.geoloop);

          std::vector<GeoLoop> holes;
          std::vector<duckdb::shared_ptr<std::vector<LatLng>>> holesVerts;
          while (strIndex < str.length() && str[strIndex] == ',') {
            strIndex++;
            strIndex = whitespace(str, strIndex);
            if (str[strIndex] == '(') {
              GeoLoop hole;
              auto verts = duckdb::make_shared_ptr<std::vector<LatLng>>();
              strIndex = readGeoLoop(str, strIndex, verts, hole);
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

          int64_t numCells = 0;
          H3Error err = maxPolygonToCellsSize(&polygon, res, flags, &numCells);
          if (err) {
            return list_entry_t(offset, 0);
          } else {
            std::vector<H3Index> out(numCells);
            H3Error err2 = polygonToCells(&polygon, res, flags, out.data());
            if (err2) {
              return list_entry_t(offset, 0);
            } else {
              uint64_t actual = 0;
              for (H3Index outCell : out) {
                if (outCell != H3_NULL) {
                  ListVector::PushBack(result, Value::UBIGINT(outCell));
                  actual++;
                }
              }
              return list_entry_t(offset, actual);
            }
          }
        }
        return list_entry_t(offset, 0);
      });
}

static void PolygonWktToCellsVarcharFunction(DataChunk &args,
                                             ExpressionState &state,
                                             Vector &result) {
  // TODO: Note this function is not fully noexcept -- some invalid WKT strings
  // will throw, others will return empty lists.
  BinaryExecutor::Execute<string_t, int, list_entry_t>(
      args.data[0], args.data[1], result, args.size(),
      [&](string_t input, int res) {
        GeoPolygon polygon;
        int32_t flags = 0;

        std::string str = input.GetString();

        uint64_t offset = ListVector::GetListSize(result);
        if (str.rfind(POLYGON, 0) != 0) {
          return list_entry_t(offset, 0);
        }

        size_t strIndex = POLYGON.length();
        strIndex = whitespace(str, strIndex);

        if (str.rfind(EMPTY, strIndex) == strIndex) {
          return list_entry_t(offset, 0);
        }

        if (str[strIndex] == '(') {
          strIndex++;
          strIndex = whitespace(str, strIndex);

          auto outerVerts = duckdb::make_shared_ptr<std::vector<LatLng>>();
          strIndex = readGeoLoop(str, strIndex, outerVerts, polygon.geoloop);

          std::vector<GeoLoop> holes;
          std::vector<duckdb::shared_ptr<std::vector<LatLng>>> holesVerts;
          while (strIndex < str.length() && str[strIndex] == ',') {
            strIndex++;
            strIndex = whitespace(str, strIndex);
            if (str[strIndex] == '(') {
              GeoLoop hole;
              auto verts = duckdb::make_shared_ptr<std::vector<LatLng>>();
              strIndex = readGeoLoop(str, strIndex, verts, hole);
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

          int64_t numCells = 0;
          H3Error err = maxPolygonToCellsSize(&polygon, res, flags, &numCells);
          if (err) {
            return list_entry_t(offset, 0);
          } else {
            std::vector<H3Index> out(numCells);
            H3Error err2 = polygonToCells(&polygon, res, flags, out.data());
            if (err2) {
              return list_entry_t(offset, 0);
            } else {
              uint64_t actual = 0;
              for (H3Index outCell : out) {
                if (outCell != H3_NULL) {
                  auto str = StringUtil::Format("%llx", outCell);
                  ListVector::PushBack(result, str);
                  actual++;
                }
              }
              return list_entry_t(offset, actual);
            }
          }
        }
        return list_entry_t(offset, 0);
      });
}

static list_entry_t
PolygonWktToCellsExperimentalInnerFunction(string_t input, int res,
                                           string_t flagsStr, Vector &result) {
  // TODO: Note this function is not fully noexcept -- some invalid WKT strings
  // will throw, others will return empty lists.
  GeoPolygon polygon;
  int32_t flags = 0;

  std::string str = input.GetString();

  uint64_t offset = ListVector::GetListSize(result);

  // TODO: Make flags easier to work with
  if (flagsStr == "CONTAINMENT_CENTER" || flagsStr == "center") {
    flags = 0;
  } else if (flagsStr == "CONTAINMENT_FULL" || flagsStr == "full") {
    flags = 1;
  } else if (flagsStr == "CONTAINMENT_OVERLAPPING" || flagsStr == "overlap") {
    flags = 2;
  } else if (flagsStr == "CONTAINMENT_OVERLAPPING_BBOX" ||
             flagsStr == "overlap_bbox") {
    flags = 3;
  } else {
    // Invalid flags input
    return list_entry_t(offset, 0);
  }

  if (str.rfind(POLYGON, 0) != 0) {
    return list_entry_t(offset, 0);
  }

  size_t strIndex = POLYGON.length();
  strIndex = whitespace(str, strIndex);

  if (str.rfind(EMPTY, strIndex) == strIndex) {
    return list_entry_t(offset, 0);
  }

  if (str[strIndex] == '(') {
    strIndex++;
    strIndex = whitespace(str, strIndex);

    auto outerVerts = duckdb::make_shared_ptr<std::vector<LatLng>>();
    strIndex = readGeoLoop(str, strIndex, outerVerts, polygon.geoloop);

    std::vector<GeoLoop> holes;
    std::vector<duckdb::shared_ptr<std::vector<LatLng>>> holesVerts;
    while (strIndex < str.length() && str[strIndex] == ',') {
      strIndex++;
      strIndex = whitespace(str, strIndex);
      if (str[strIndex] == '(') {
        GeoLoop hole;
        auto verts = duckdb::make_shared_ptr<std::vector<LatLng>>();
        strIndex = readGeoLoop(str, strIndex, verts, hole);
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

    int64_t numCells = 0;
    H3Error err =
        maxPolygonToCellsSizeExperimental(&polygon, res, flags, &numCells);
    if (err) {
      return list_entry_t(offset, 0);
    } else {
      std::vector<H3Index> out(numCells);
      H3Error err2 = polygonToCellsExperimental(&polygon, res, flags, numCells,
                                                out.data());
      if (err2) {
        return list_entry_t(offset, 0);
      } else {
        uint64_t actual = 0;
        for (H3Index outCell : out) {
          if (outCell != H3_NULL) {
            ListVector::PushBack(result, Value::UBIGINT(outCell));
            actual++;
          }
        }
        return list_entry_t(offset, actual);
      }
    }
  }
  return list_entry_t(offset, 0);
}

static void PolygonWktToCellsExperimentalFunction(DataChunk &args,
                                                  ExpressionState &state,
                                                  Vector &result) {
  TernaryExecutor::Execute<string_t, int, string_t, list_entry_t>(
      args.data[0], args.data[1], args.data[2], result, args.size(),
      [&](string_t input, int res, string_t flagsStr) {
        return PolygonWktToCellsExperimentalInnerFunction(input, res, flagsStr,
                                                          result);
      });
}

static void PolygonWktToCellsExperimentalFunctionSwapped(DataChunk &args,
                                                         ExpressionState &state,
                                                         Vector &result) {
  TernaryExecutor::Execute<string_t, string_t, int, list_entry_t>(
      args.data[0], args.data[1], args.data[2], result, args.size(),
      [&](string_t input, string_t flagsStr, int res) {
        return PolygonWktToCellsExperimentalInnerFunction(input, res, flagsStr,
                                                          result);
      });
}

static list_entry_t PolygonWktToCellsExperimentalVarcharInnerFunction(
    string_t input, int res, string_t flagsStr, Vector &result) {
  // TODO: Note this function is not fully noexcept -- some invalid WKT strings
  // will throw, others will return empty lists.
  GeoPolygon polygon;
  int32_t flags = 0;

  std::string str = input.GetString();

  uint64_t offset = ListVector::GetListSize(result);

  // TODO: Make flags easier to work with
  if (flagsStr == "CONTAINMENT_CENTER" || flagsStr == "center") {
    flags = 0;
  } else if (flagsStr == "CONTAINMENT_FULL" || flagsStr == "full") {
    flags = 1;
  } else if (flagsStr == "CONTAINMENT_OVERLAPPING" || flagsStr == "overlap") {
    flags = 2;
  } else if (flagsStr == "CONTAINMENT_OVERLAPPING_BBOX" ||
             flagsStr == "overlap_bbox") {
    flags = 3;
  } else {
    // Invalid flags input
    return list_entry_t(offset, 0);
  }

  if (str.rfind(POLYGON, 0) != 0) {
    return list_entry_t(offset, 0);
  }

  size_t strIndex = POLYGON.length();
  strIndex = whitespace(str, strIndex);

  if (str.rfind(EMPTY, strIndex) == strIndex) {
    return list_entry_t(offset, 0);
  }

  if (str[strIndex] == '(') {
    strIndex++;
    strIndex = whitespace(str, strIndex);

    auto outerVerts = duckdb::make_shared_ptr<std::vector<LatLng>>();
    strIndex = readGeoLoop(str, strIndex, outerVerts, polygon.geoloop);

    std::vector<GeoLoop> holes;
    std::vector<duckdb::shared_ptr<std::vector<LatLng>>> holesVerts;
    while (strIndex < str.length() && str[strIndex] == ',') {
      strIndex++;
      strIndex = whitespace(str, strIndex);
      if (str[strIndex] == '(') {
        GeoLoop hole;
        auto verts = duckdb::make_shared_ptr<std::vector<LatLng>>();
        strIndex = readGeoLoop(str, strIndex, verts, hole);
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

    int64_t numCells = 0;
    H3Error err =
        maxPolygonToCellsSizeExperimental(&polygon, res, flags, &numCells);
    if (err) {
      return list_entry_t(offset, 0);
    } else {
      std::vector<H3Index> out(numCells);
      H3Error err2 = polygonToCellsExperimental(&polygon, res, flags, numCells,
                                                out.data());
      if (err2) {
        return list_entry_t(offset, 0);
      } else {
        uint64_t actual = 0;
        for (H3Index outCell : out) {
          if (outCell != H3_NULL) {
            auto str = StringUtil::Format("%llx", outCell);
            ListVector::PushBack(result, str);
            actual++;
          }
        }
        return list_entry_t(offset, actual);
      }
    }
  }
  return list_entry_t(offset, 0);
}

static void PolygonWktToCellsExperimentalVarcharFunction(DataChunk &args,
                                                         ExpressionState &state,
                                                         Vector &result) {
  TernaryExecutor::Execute<string_t, int, string_t, list_entry_t>(
      args.data[0], args.data[1], args.data[2], result, args.size(),
      [&](string_t input, int res, string_t flagsStr) {
        return PolygonWktToCellsExperimentalVarcharInnerFunction(
            input, res, flagsStr, result);
      });
}

static void PolygonWktToCellsExperimentalVarcharFunctionSwapped(
    DataChunk &args, ExpressionState &state, Vector &result) {
  TernaryExecutor::Execute<string_t, string_t, int, list_entry_t>(
      args.data[0], args.data[1], args.data[2], result, args.size(),
      [&](string_t input, string_t flagsStr, int res) {
        return PolygonWktToCellsExperimentalVarcharInnerFunction(
            input, res, flagsStr, result);
      });
}

static list_entry_t
PolygonWkbToCellsExperimentalInnerFunction(string_t input, int res,
                                           string_t flagsStr, Vector &result) {
  // TODO: Note this function is not fully noexcept -- some invalid WKT strings
  // will throw, others will return empty lists.
  int32_t flags = 0;

  uint64_t offset = ListVector::GetListSize(result);

  // TODO: Make flags easier to work with
  if (flagsStr == "CONTAINMENT_CENTER" || flagsStr == "center") {
    flags = 0;
  } else if (flagsStr == "CONTAINMENT_FULL" || flagsStr == "full") {
    flags = 1;
  } else if (flagsStr == "CONTAINMENT_OVERLAPPING" || flagsStr == "overlap") {
    flags = 2;
  } else if (flagsStr == "CONTAINMENT_OVERLAPPING_BBOX" ||
             flagsStr == "overlap_bbox") {
    flags = 3;
  } else {
    // Invalid flags input
    return list_entry_t(offset, 0);
  }

  auto outerVerts = duckdb::make_shared_ptr<std::vector<LatLng>>();
  std::vector<GeoLoop> holes;
  std::vector<duckdb::shared_ptr<std::vector<LatLng>>> holesVerts;
  GeoPolygon polygon = {0};
  DecodeWkbPolygon(input, polygon, outerVerts, holes, holesVerts);
  if (polygon.geoloop.numVerts > 0) {
    int64_t numCells = 0;
    H3Error err =
        maxPolygonToCellsSizeExperimental(&polygon, res, flags, &numCells);
    if (err) {
      return list_entry_t(offset, 0);
    } else {
      std::vector<H3Index> out(numCells);
      H3Error err2 = polygonToCellsExperimental(&polygon, res, flags, numCells,
                                                out.data());
      if (err2) {
        return list_entry_t(offset, 0);
      } else {
        uint64_t actual = 0;
        for (H3Index outCell : out) {
          if (outCell != H3_NULL) {
            ListVector::PushBack(result, Value::UBIGINT(outCell));
            actual++;
          }
        }
        return list_entry_t(offset, actual);
      }
    }
  }
  return list_entry_t(offset, 0);
}

static void PolygonWkbToCellsExperimentalFunction(DataChunk &args,
                                                  ExpressionState &state,
                                                  Vector &result) {
  TernaryExecutor::Execute<string_t, int, string_t, list_entry_t>(
      args.data[0], args.data[1], args.data[2], result, args.size(),
      [&](string_t input, int res, string_t flagsStr) {
        return PolygonWkbToCellsExperimentalInnerFunction(input, res, flagsStr,
                                                          result);
      });
}

static void PolygonWkbToCellsExperimentalFunctionSwapped(DataChunk &args,
                                                         ExpressionState &state,
                                                         Vector &result) {
  TernaryExecutor::Execute<string_t, string_t, int, list_entry_t>(
      args.data[0], args.data[1], args.data[2], result, args.size(),
      [&](string_t input, string_t flagsStr, int res) {
        return PolygonWkbToCellsExperimentalInnerFunction(input, res, flagsStr,
                                                          result);
      });
}

CreateScalarFunctionInfo H3Functions::GetCellsToMultiPolygonWktFunction() {
  ScalarFunctionSet funcs("h3_cells_to_multi_polygon_wkt");
  funcs.AddFunction(ScalarFunction(
      {LogicalType::LIST(LogicalType::VARCHAR)}, LogicalType::VARCHAR,
      CellsToMultiPolygonFunction<string_t,
                                  CellsToMultiPolygonVarcharInputOperator,
                                  WktEncoder, false>));
  funcs.AddFunction(ScalarFunction(
      {LogicalType::LIST(LogicalType::UBIGINT)}, LogicalType::VARCHAR,
      CellsToMultiPolygonFunction<uint64_t, CellsToMultiPolygonInputOperator,
                                  WktEncoder, false>));
  funcs.AddFunction(ScalarFunction(
      {LogicalType::LIST(LogicalType::BIGINT)}, LogicalType::VARCHAR,
      CellsToMultiPolygonFunction<int64_t, CellsToMultiPolygonInputOperator,
                                  WktEncoder, false>));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetCellsToMultiPolygonWkbFunction() {
  ScalarFunctionSet funcs("h3_cells_to_multi_polygon_wkb");
  funcs.AddFunction(ScalarFunction(
      {LogicalType::LIST(LogicalType::VARCHAR)}, LogicalType::BLOB,
      CellsToMultiPolygonFunction<string_t,
                                  CellsToMultiPolygonVarcharInputOperator,
                                  WkbEncoder, true>));
  funcs.AddFunction(ScalarFunction(
      {LogicalType::LIST(LogicalType::UBIGINT)}, LogicalType::BLOB,
      CellsToMultiPolygonFunction<uint64_t, CellsToMultiPolygonInputOperator,
                                  WkbEncoder, true>));
  funcs.AddFunction(ScalarFunction(
      {LogicalType::LIST(LogicalType::BIGINT)}, LogicalType::BLOB,
      CellsToMultiPolygonFunction<int64_t, CellsToMultiPolygonInputOperator,
                                  WkbEncoder, true>));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetPolygonWktToCellsFunction() {
  // TODO: Expose flags
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_polygon_wkt_to_cells", {LogicalType::VARCHAR, LogicalType::INTEGER},
      LogicalType::LIST(LogicalType::UBIGINT), PolygonWktToCellsFunction));
}

CreateScalarFunctionInfo H3Functions::GetPolygonWktToCellsVarcharFunction() {
  // TODO: Expose flags
  return CreateScalarFunctionInfo(
      ScalarFunction("h3_polygon_wkt_to_cells_string",
                     {LogicalType::VARCHAR, LogicalType::INTEGER},
                     LogicalType::LIST(LogicalType::VARCHAR),
                     PolygonWktToCellsVarcharFunction));
}

CreateScalarFunctionInfo
H3Functions::GetPolygonWktToCellsExperimentalFunction() {
  ScalarFunctionSet funcs("h3_polygon_wkt_to_cells_experimental");
  funcs.AddFunction(ScalarFunction(
      {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR},
      LogicalType::LIST(LogicalType::UBIGINT),
      PolygonWktToCellsExperimentalFunction));
  funcs.AddFunction(ScalarFunction(
      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER},
      LogicalType::LIST(LogicalType::UBIGINT),
      PolygonWktToCellsExperimentalFunctionSwapped));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo
H3Functions::GetPolygonWkbToCellsExperimentalFunction() {
  ScalarFunctionSet funcs("h3_polygon_wkb_to_cells_experimental");
  funcs.AddFunction(ScalarFunction(
      {LogicalType::BLOB, LogicalType::INTEGER, LogicalType::VARCHAR},
      LogicalType::LIST(LogicalType::UBIGINT),
      PolygonWkbToCellsExperimentalFunction));
  funcs.AddFunction(ScalarFunction(
      {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER},
      LogicalType::LIST(LogicalType::UBIGINT),
      PolygonWkbToCellsExperimentalFunctionSwapped));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo
H3Functions::GetPolygonWktToCellsExperimentalVarcharFunction() {
  ScalarFunctionSet funcs("h3_polygon_wkt_to_cells_experimental_string");
  funcs.AddFunction(ScalarFunction(
      {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR},
      LogicalType::LIST(LogicalType::VARCHAR),
      PolygonWktToCellsExperimentalVarcharFunction));
  funcs.AddFunction(ScalarFunction(
      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER},
      LogicalType::LIST(LogicalType::VARCHAR),
      PolygonWktToCellsExperimentalVarcharFunctionSwapped));
  return CreateScalarFunctionInfo(funcs);
}

} // namespace duckdb
