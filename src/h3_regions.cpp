#include "h3_common.hpp"
#include "h3_functions.hpp"
#include "well_known_encoder.hpp"
#include "well_known_decoder.hpp"

namespace h3duckdb {

static uint32_t StringToFlags(duckdb_string_t *flags) {
  // TODO: Make flags easier to work with
  auto flagsStr = DuckdbToString(flags);
  if (flagsStr == "CONTAINMENT_CENTER" || flagsStr == "center") {
    return 0;
  } else if (flagsStr == "CONTAINMENT_FULL" || flagsStr == "full") {
    return 1;
  } else if (flagsStr == "CONTAINMENT_OVERLAPPING" || flagsStr == "overlap") {
    return 2;
  } else if (flagsStr == "CONTAINMENT_OVERLAPPING_BBOX" ||
             flagsStr == "overlap_bbox") {
    return 3;
  } else {
    // Invalid flags input
    return UINT32_MAX;
  }
}

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

template <typename T, class Encoder>
void CellsToMultiPolygonFunction(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  duckdb_list_entry *indexVecData =
      (duckdb_list_entry *)duckdb_vector_get_data(indexVec);
  duckdb_vector indexChildVec = duckdb_list_vector_get_child(indexVec);
  T *indexChildVecData = (T *)duckdb_vector_get_data(indexChildVec);
  uint64_t *indexChildValidity = duckdb_vector_get_validity(indexChildVec);

  duckdb_vector_ensure_validity_writable(output);
  duckdb_string_t *resultData =
      (duckdb_string_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool hasNullInput = false;

    std::vector<H3Index> inputSet(indexVecData[row].length);
    for (idx_t j = 0; j < indexVecData[row].length; ++j) {
      auto childRow = indexVecData[row].offset + j;
      if (duckdb_validity_row_is_valid(indexChildValidity, childRow)) {
        auto cell = IndexFromVector(indexChildVecData, childRow);

        if (cell) {
          inputSet[j] = cell;
        } else {
          hasNullInput = true;
          break;
        }
      } else {
        hasNullInput = true;
        break;
      }
    }

    LinkedGeoPolygon firstLgp = {0};
    H3Error err =
        hasNullInput ||
        cellsToLinkedMultiPolygon(inputSet.data(), inputSet.size(), &firstLgp);
    if (!err) {
      auto enc = Encoder();
      auto polygon_count = PolygonCount(&firstLgp);
      enc.StartMultiPolygon(polygon_count);

      if (firstLgp.first) {
        LinkedGeoPolygon *lgp = &firstLgp;
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
      duckdb_vector_assign_string_element_len(output, row, str.c_str(),
                                              str.size());

      destroyLinkedMultiPolygon(&firstLgp);
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

//
// static list_entry_t PolygonToCellsExperimental(Vector &result,
//                                               GeoPolygon &polygon, int res,
//                                               uint32_t flags) {
//  uint64_t offset = ListVector::GetListSize(result);
//  if (polygon.geoloop.numVerts > 0) {
//    int64_t numCells = 0;
//    H3Error err =
//        maxPolygonToCellsSizeExperimental(&polygon, res, flags, &numCells);
//    if (err) {
//      return list_entry_t(offset, 0);
//    } else {
//      std::vector<H3Index> out(numCells);
//      H3Error err2 = polygonToCellsExperimental(&polygon, res, flags,
//      numCells,
//                                                out.data());
//      if (err2) {
//        return list_entry_t(offset, 0);
//      } else {
//        uint64_t actual = 0;
//        for (H3Index outCell : out) {
//          if (outCell != H3_NULL) {
//            ListVector::PushBack(result, Value::UBIGINT(outCell));
//            actual++;
//          }
//        }
//        return list_entry_t(offset, actual);
//      }
//    }
//  }
//  return list_entry_t(offset, 0);
//}
//

template <typename T, bool IsWkb>
void PolygonWktOrWkbToCellsFunction(duckdb_function_info info,
                                    duckdb_data_chunk input,
                                    duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector wellKnownVec = duckdb_data_chunk_get_vector(input, 0);
  duckdb_string_t *wellKnownVecData =
      (duckdb_string_t *)duckdb_vector_get_data(wellKnownVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *resData = (int32_t *)duckdb_vector_get_data(resVec);

  std::vector<std::pair<bool, std::vector<H3Index>>> results;
  idx_t outputReserveSize = 0;
  for (idx_t row = 0; row < inputSize; ++row) {
    bool hasData = false;
    auto inputStr = DuckdbToString(&wellKnownVecData[row]);
    GeoPolygon polygon = {0};
    int32_t flags = 0;
    int32_t res = resData[row];

    std::vector<LatLng> outerVerts;
    std::vector<GeoLoop> holes;
    std::vector<std::vector<LatLng>> holesVerts;
    std::vector<H3Index> resultsTmp;
    try {
      if (IsWkb) {
        DecodeWkbPolygon(inputStr, polygon, outerVerts, holes, holesVerts);
      } else {
        DecodeWktPolygon(inputStr, polygon, outerVerts, holes, holesVerts);
      }
    } catch (H3Exception ex) {
      results.push_back(std::make_pair(false, resultsTmp));
      continue;
    }

    if (polygon.geoloop.numVerts > 0) {
      int64_t numCells = 0;
      H3Error err = maxPolygonToCellsSize(&polygon, res, flags, &numCells);
      if (!err) {
        std::vector<H3Index> out(numCells);
        H3Error err2 = polygonToCells(&polygon, res, flags, out.data());
        if (!err2) {
          for (H3Index outCell : out) {
            if (outCell != H3_NULL) {
              resultsTmp.push_back(outCell);
            }
          }
          hasData = true;
        }
      }
    }

    results.push_back(std::make_pair(hasData, resultsTmp));
  }

  duckdb_list_vector_reserve(output, outputReserveSize);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  T *resultData = (T *)duckdb_vector_get_data(outputChildVec);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;

  for (idx_t row = 0; row < inputSize; ++row) {
    auto [hasData, resultsTmp] = results[row];

    if (hasData) {
      for (idx_t j = 0; j < resultsTmp.size(); ++j) {
        auto out = resultsTmp[j];
        auto childRowOffset = resultOffset + j;
        AssignHexString(outputChildVec, resultData, childRowOffset, out);
      }

      entries[row].offset = resultOffset;
      entries[row].length = resultsTmp.size();
      resultOffset += resultsTmp.size();
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

//
// static list_entry_t
// PolygonWktToCellsExperimentalInnerFunction(string_t input, int res,
//                                           string_t flagsStr, Vector &result)
//                                           {
//  // TODO: Note this function is not fully noexcept -- some invalid WKT
//  strings
//      // will throw, others will return empty lists.
//      GeoPolygon polygon = {0};
//
//  uint64_t offset = ListVector::GetListSize(result);
//  uint32_t flags = StringToFlags(flagsStr);
//  if (flags == UINT32_MAX) {
//    // Invalid flags input
//    return list_entry_t(offset, 0);
//  }
//
//  auto outerVerts = duckdb::make_shared_ptr<std::vector<LatLng>>();
//  std::vector<GeoLoop> holes;
//  std::vector<duckdb::shared_ptr<std::vector<LatLng>>> holesVerts;
//  DecodeWktPolygon(input, polygon, outerVerts, holes, holesVerts);
//
//  return PolygonToCellsExperimental(result, polygon, res, flags);
//}
//
// static void PolygonWktToCellsExperimentalFunction(DataChunk &args,
//                                                  ExpressionState &state,
//                                                  Vector &result) {
//  TernaryExecutor::Execute<string_t, int, string_t, list_entry_t>(
//      args.data[0], args.data[1], args.data[2], result, args.size(),
//      [&](string_t input, int res, string_t flagsStr) {
//        return PolygonWktToCellsExperimentalInnerFunction(input, res,
//        flagsStr,
//                                                          result);
//      });
//}
//
// static void PolygonWktToCellsExperimentalFunctionSwapped(DataChunk &args,
//                                                         ExpressionState
//                                                         &state, Vector
//                                                         &result) {
//  TernaryExecutor::Execute<string_t, string_t, int, list_entry_t>(
//      args.data[0], args.data[1], args.data[2], result, args.size(),
//      [&](string_t input, string_t flagsStr, int res) {
//        return PolygonWktToCellsExperimentalInnerFunction(input, res,
//        flagsStr,
//                                                          result);
//      });
//}
//
// static list_entry_t
// PolygonWkbToCellsExperimentalInnerFunction(string_t input, int res,
//                                           string_t flagsStr, Vector &result)
//                                           {
//  // TODO: Note this function is not fully noexcept -- some invalid WKB
//  strings
//      // will throw, others will return empty lists.
//
//      uint64_t offset = ListVector::GetListSize(result);
//
//  uint32_t flags = StringToFlags(flagsStr);
//  if (flags == UINT32_MAX) {
//    // Invalid flags input
//    return list_entry_t(offset, 0);
//  }
//
//  auto outerVerts = duckdb::make_shared_ptr<std::vector<LatLng>>();
//  std::vector<GeoLoop> holes;
//  std::vector<duckdb::shared_ptr<std::vector<LatLng>>> holesVerts;
//  GeoPolygon polygon = {0};
//  DecodeWkbPolygon(input, polygon, outerVerts, holes, holesVerts);
//  return PolygonToCellsExperimental(result, polygon, res, flags);
//}
//
// static void PolygonWkbToCellsExperimentalFunction(DataChunk &args,
//                                                  ExpressionState &state,
//                                                  Vector &result) {
//  TernaryExecutor::Execute<string_t, int, string_t, list_entry_t>(
//      args.data[0], args.data[1], args.data[2], result, args.size(),
//      [&](string_t input, int res, string_t flagsStr) {
//        return PolygonWkbToCellsExperimentalInnerFunction(input, res,
//        flagsStr,
//                                                          result);
//      });
//}
//
// static void PolygonWkbToCellsExperimentalFunctionSwapped(DataChunk &args,
//                                                         ExpressionState
//                                                         &state, Vector
//                                                         &result) {
//  TernaryExecutor::Execute<string_t, string_t, int, list_entry_t>(
//      args.data[0], args.data[1], args.data[2], result, args.size(),
//      [&](string_t input, string_t flagsStr, int res) {
//        return PolygonWkbToCellsExperimentalInnerFunction(input, res,
//        flagsStr,
//                                                          result);
//      });
//}

duckdb_scalar_function_set H3Functions::GetCellsToMultiPolygonWktFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cells_to_multi_polygon_wkt");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

  auto r = [&functionSet,
            &varcharType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);
    duckdb_logical_type listType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cells_to_multi_polygon_wkt");
    duckdb_scalar_function_add_parameter(function, listType);
    duckdb_scalar_function_set_return_type(function, varcharType);
    duckdb_scalar_function_set_function(
        function, CellsToMultiPolygonFunction<PhysicalType, WktEncoder>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&listType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&varcharType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCellsToMultiPolygonWkbFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cells_to_multi_polygon_wkb");

  duckdb_logical_type blobType = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);

  auto r = [&functionSet,
            &blobType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);
    duckdb_logical_type listType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cells_to_multi_polygon_wkb");
    duckdb_scalar_function_add_parameter(function, listType);
    duckdb_scalar_function_set_return_type(function, blobType);
    duckdb_scalar_function_set_function(
        function, CellsToMultiPolygonFunction<PhysicalType, WkbEncoder>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&listType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&blobType);

  return functionSet;
}

duckdb_scalar_function H3Functions::GetPolygonWktToCellsFunction() {
  // TODO: Expose flags
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_polygon_wkt_to_cells");
  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type ubigintListType = duckdb_create_list_type(ubigintType);
  duckdb_scalar_function_add_parameter(function, varcharType);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, ubigintListType);
  duckdb_destroy_logical_type(&ubigintListType);
  duckdb_destroy_logical_type(&ubigintType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_scalar_function_set_function(
      function, PolygonWktOrWkbToCellsFunction<uint64_t, false>);
  return function;
}

duckdb_scalar_function H3Functions::GetPolygonWktToCellsVarcharFunction() {
  // TODO: Expose flags
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_polygon_wkt_to_cells_string");
  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type varcharListType = duckdb_create_list_type(varcharType);
  duckdb_scalar_function_add_parameter(function, varcharType);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, varcharListType);
  duckdb_destroy_logical_type(&varcharListType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_scalar_function_set_function(
      function, PolygonWktOrWkbToCellsFunction<duckdb_string_t, false>);
  return function;
}

duckdb_scalar_function H3Functions::GetPolygonWkbToCellsFunction() {
  // TODO: Expose flags
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_polygon_wkb_to_cells");
  duckdb_logical_type blobType = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type ubigintListType = duckdb_create_list_type(ubigintType);
  duckdb_scalar_function_add_parameter(function, blobType);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, ubigintListType);
  duckdb_destroy_logical_type(&ubigintListType);
  duckdb_destroy_logical_type(&ubigintType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&blobType);
  duckdb_scalar_function_set_function(
      function, PolygonWktOrWkbToCellsFunction<uint64_t, true>);
  return function;
}

duckdb_scalar_function H3Functions::GetPolygonWkbToCellsVarcharFunction() {
  // TODO: Expose flags
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_polygon_wkb_to_cells_string");
  duckdb_logical_type blobType = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type varcharListType = duckdb_create_list_type(varcharType);
  duckdb_scalar_function_add_parameter(function, blobType);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, varcharListType);
  duckdb_destroy_logical_type(&varcharListType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&blobType);
  duckdb_scalar_function_set_function(
      function, PolygonWktOrWkbToCellsFunction<duckdb_string_t, true>);
  return function;
}

// CreateScalarFunctionInfo
// H3Functions::GetPolygonWktToCellsExperimentalFunction() {
//  ScalarFunctionSet funcs("h3_polygon_wkt_to_cells_experimental");
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR},
//      LogicalType::LIST(LogicalType::UBIGINT),
//      PolygonWktToCellsExperimentalFunction));
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER},
//      LogicalType::LIST(LogicalType::UBIGINT),
//      PolygonWktToCellsExperimentalFunctionSwapped));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo
// H3Functions::GetPolygonWkbToCellsExperimentalFunction() {
//  ScalarFunctionSet funcs("h3_polygon_wkb_to_cells_experimental");
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::BLOB, LogicalType::INTEGER, LogicalType::VARCHAR},
//      LogicalType::LIST(LogicalType::UBIGINT),
//      PolygonWkbToCellsExperimentalFunction));
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER},
//      LogicalType::LIST(LogicalType::UBIGINT),
//      PolygonWkbToCellsExperimentalFunctionSwapped));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo
// H3Functions::GetPolygonWktToCellsExperimentalVarcharFunction() {
//  ScalarFunctionSet funcs("h3_polygon_wkt_to_cells_experimental_string");
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR},
//      LogicalType::LIST(LogicalType::VARCHAR),
//      PolygonWktToCellsExperimentalVarcharFunction));
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER},
//      LogicalType::LIST(LogicalType::VARCHAR),
//      PolygonWktToCellsExperimentalVarcharFunctionSwapped));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo
// H3Functions::GetPolygonWkbToCellsExperimentalVarcharFunction() {
//  ScalarFunctionSet funcs("h3_polygon_wkb_to_cells_experimental_string");
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::BLOB, LogicalType::INTEGER, LogicalType::VARCHAR},
//      LogicalType::LIST(LogicalType::VARCHAR),
//      PolygonWkbToCellsExperimentalVarcharFunction));
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::BLOB, LogicalType::VARCHAR, LogicalType::INTEGER},
//      LogicalType::LIST(LogicalType::VARCHAR),
//      PolygonWkbToCellsExperimentalVarcharFunctionSwapped));
//  return CreateScalarFunctionInfo(funcs);
//}

} // namespace h3duckdb
