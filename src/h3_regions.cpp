#include "h3_common.hpp"
#include "h3_functions.hpp"
#include "well_known_encoder.hpp"
#include "well_known_decoder.hpp"

namespace h3duckdb {

static uint32_t StringToFlags(const std::string &flagsStr) {
  // TODO: Make flags easier to work with
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

    auto outerVerts = std::make_shared<std::vector<LatLng>>();
    std::vector<GeoLoop> holes;
    std::vector<std::shared_ptr<std::vector<LatLng>>> holesVerts;
    std::vector<H3Index> resultsTmp;
    try {
      if (IsWkb) {
        DecodeWkbPolygon(inputStr, polygon, outerVerts, holes, holesVerts);
      } else {
        DecodeWktPolygon(inputStr, polygon, outerVerts, holes, holesVerts);
      }
    } catch (H3Exception ex) {
      duckdb_scalar_function_set_error(info, ex.what());
      return;
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
    } else {
      hasData = true;
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

template <typename T, bool IsWkb, bool SwapFlagsRes>
void PolygonWktOrWkbToCellsExperimentalFunction(duckdb_function_info info,
                                                duckdb_data_chunk input,
                                                duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector wellKnownVec = duckdb_data_chunk_get_vector(input, 0);
  duckdb_string_t *wellKnownVecData =
      (duckdb_string_t *)duckdb_vector_get_data(wellKnownVec);
  duckdb_vector resVec =
      duckdb_data_chunk_get_vector(input, SwapFlagsRes ? 2 : 1);
  int32_t *resData = (int32_t *)duckdb_vector_get_data(resVec);
  duckdb_vector flagsVec =
      duckdb_data_chunk_get_vector(input, SwapFlagsRes ? 1 : 2);
  duckdb_string_t *flagsData =
      (duckdb_string_t *)duckdb_vector_get_data(flagsVec);

  std::vector<std::pair<bool, std::vector<H3Index>>> results;
  idx_t outputReserveSize = 0;
  for (idx_t row = 0; row < inputSize; ++row) {
    bool hasData = false;
    auto inputStr = DuckdbToString(&wellKnownVecData[row]);
    GeoPolygon polygon = {0};
    auto flagsStr = DuckdbToString(&flagsData[row]);
    int32_t res = resData[row];

    auto outerVerts = std::make_shared<std::vector<LatLng>>();
    std::vector<GeoLoop> holes;
    std::vector<std::shared_ptr<std::vector<LatLng>>> holesVerts;
    std::vector<H3Index> resultsTmp;
    try {
      if (IsWkb) {
        DecodeWkbPolygon(inputStr, polygon, outerVerts, holes, holesVerts);
      } else {
        DecodeWktPolygon(inputStr, polygon, outerVerts, holes, holesVerts);
      }
    } catch (H3Exception ex) {
      duckdb_scalar_function_set_error(info, ex.what());
      return;
    }

    uint32_t flags = StringToFlags(flagsStr);

    // Invalid flags input
    if (polygon.geoloop.numVerts > 0 && flags != UINT32_MAX) {
      int64_t numCells = 0;

      H3Error err =
          maxPolygonToCellsSizeExperimental(&polygon, res, flags, &numCells);
      if (!err) {
        std::vector<H3Index> out(numCells);
        H3Error err2 = polygonToCellsExperimental(&polygon, res, flags,
                                                  numCells, out.data());
        if (!err2) {
          for (H3Index outCell : out) {
            if (outCell != H3_NULL) {
              resultsTmp.push_back(outCell);
            }
          }
          hasData = true;
        }
      }
    } else {
      hasData = true;
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

duckdb_scalar_function_set
H3Functions::GetPolygonWktToCellsExperimentalFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_polygon_wkt_to_cells_experimental");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type ubigintListType = duckdb_create_list_type(ubigintType);

  auto r = [&functionSet, &varcharType, &intType,
            &ubigintListType]<bool SwapFlagsRes>() {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function,
                                    "h3_polygon_wkt_to_cells_experimental");
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_add_parameter(function,
                                         SwapFlagsRes ? varcharType : intType);
    duckdb_scalar_function_add_parameter(function,
                                         SwapFlagsRes ? intType : varcharType);
    duckdb_scalar_function_set_return_type(function, ubigintListType);
    duckdb_scalar_function_set_function(
        function, PolygonWktOrWkbToCellsExperimentalFunction<uint64_t, false,
                                                             SwapFlagsRes>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  };

  r.operator()<false>();
  r.operator()<true>();

  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&ubigintListType);
  duckdb_destroy_logical_type(&ubigintType);

  return functionSet;
}

duckdb_scalar_function_set
H3Functions::GetPolygonWkbToCellsExperimentalFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_polygon_wkb_to_cells_experimental");

  duckdb_logical_type blobType = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type ubigintListType = duckdb_create_list_type(ubigintType);

  auto r = [&functionSet, &blobType, &varcharType, &intType,
            &ubigintListType]<bool SwapFlagsRes>() {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function,
                                    "h3_polygon_wkb_to_cells_experimental");
    duckdb_scalar_function_add_parameter(function, blobType);
    duckdb_scalar_function_add_parameter(function,
                                         SwapFlagsRes ? varcharType : intType);
    duckdb_scalar_function_add_parameter(function,
                                         SwapFlagsRes ? intType : varcharType);
    duckdb_scalar_function_set_return_type(function, ubigintListType);
    duckdb_scalar_function_set_function(
        function, PolygonWktOrWkbToCellsExperimentalFunction<uint64_t, true,
                                                             SwapFlagsRes>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  };

  r.operator()<false>();
  r.operator()<true>();

  duckdb_destroy_logical_type(&blobType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&ubigintListType);
  duckdb_destroy_logical_type(&ubigintType);

  return functionSet;
}

duckdb_scalar_function_set
H3Functions::GetPolygonWktToCellsExperimentalVarcharFunction() {
  duckdb_scalar_function_set functionSet = duckdb_create_scalar_function_set(
      "h3_polygon_wkt_to_cells_experimental_string");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type varcharListType = duckdb_create_list_type(varcharType);

  auto r = [&functionSet, &varcharType, &intType,
            &varcharListType]<bool SwapFlagsRes>() {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(
        function, "h3_polygon_wkt_to_cells_experimental_string");
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_add_parameter(function,
                                         SwapFlagsRes ? varcharType : intType);
    duckdb_scalar_function_add_parameter(function,
                                         SwapFlagsRes ? intType : varcharType);
    duckdb_scalar_function_set_return_type(function, varcharListType);
    duckdb_scalar_function_set_function(
        function,
        PolygonWktOrWkbToCellsExperimentalFunction<duckdb_string_t, false,
                                                   SwapFlagsRes>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  };

  r.operator()<false>();
  r.operator()<true>();

  duckdb_destroy_logical_type(&varcharListType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function_set
H3Functions::GetPolygonWkbToCellsExperimentalVarcharFunction() {
  duckdb_scalar_function_set functionSet = duckdb_create_scalar_function_set(
      "h3_polygon_wkb_to_cells_experimental_string");

  duckdb_logical_type blobType = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type varcharListType = duckdb_create_list_type(varcharType);

  auto r = [&functionSet, &blobType, &varcharType, &intType,
            &varcharListType]<bool SwapFlagsRes>() {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(
        function, "h3_polygon_wkb_to_cells_experimental_string");
    duckdb_scalar_function_add_parameter(function, blobType);
    duckdb_scalar_function_add_parameter(function,
                                         SwapFlagsRes ? varcharType : intType);
    duckdb_scalar_function_add_parameter(function,
                                         SwapFlagsRes ? intType : varcharType);
    duckdb_scalar_function_set_return_type(function, varcharListType);
    duckdb_scalar_function_set_function(
        function,
        PolygonWktOrWkbToCellsExperimentalFunction<duckdb_string_t, true,
                                                   SwapFlagsRes>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  };

  r.operator()<false>();
  r.operator()<true>();

  duckdb_destroy_logical_type(&varcharListType);
  duckdb_destroy_logical_type(&blobType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

} // namespace h3duckdb
