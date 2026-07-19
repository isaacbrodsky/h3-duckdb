#include "h3_functions.hpp"
#include "well_known_encoder.hpp"

namespace h3duckdb {

void LatLngToCellFunction(duckdb_function_info info, duckdb_data_chunk input,
                          duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector latVec = duckdb_data_chunk_get_vector(input, 0);
  double *latVecData = (double *)duckdb_vector_get_data(latVec);
  uint64_t *latVecValidity = duckdb_vector_get_validity(latVec);
  duckdb_vector lngVec = duckdb_data_chunk_get_vector(input, 1);
  double *lngVecData = (double *)duckdb_vector_get_data(lngVec);
  uint64_t *lngVecValidity = duckdb_vector_get_validity(lngVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 2);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_vector_ensure_validity_writable(output);
  uint64_t *resultData = (uint64_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    if (duckdb_validity_row_is_valid(latVecValidity, row) &&
        duckdb_validity_row_is_valid(lngVecValidity, row) &&
        duckdb_validity_row_is_valid(resVecValidity, row)) {
      H3Index cell;
      double lat = latVecData[row];
      double lng = lngVecData[row];
      int32_t res = resVecData[row];
      LatLng latLng = {.lat = degsToRads(lat), .lng = degsToRads(lng)};
      H3Error err = latLngToCell(&latLng, res, &cell);
      if (!err) {
        resultData[row] = cell;
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

void LatLngToCellVarcharFunction(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector latVec = duckdb_data_chunk_get_vector(input, 0);
  double *latVecData = (double *)duckdb_vector_get_data(latVec);
  uint64_t *latVecValidity = duckdb_vector_get_validity(latVec);
  duckdb_vector lngVec = duckdb_data_chunk_get_vector(input, 1);
  double *lngVecData = (double *)duckdb_vector_get_data(lngVec);
  uint64_t *lngVecValidity = duckdb_vector_get_validity(lngVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 2);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_vector_ensure_validity_writable(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    if (duckdb_validity_row_is_valid(latVecValidity, row) &&
        duckdb_validity_row_is_valid(lngVecValidity, row) &&
        duckdb_validity_row_is_valid(resVecValidity, row)) {
      H3Index cell;
      double lat = latVecData[row];
      double lng = lngVecData[row];
      int32_t res = resVecData[row];
      LatLng latLng = {.lat = degsToRads(lat), .lng = degsToRads(lng)};
      H3Error err = latLngToCell(&latLng, res, &cell);
      if (!err) {
        std::string resultStr = ToHexString(cell);
        duckdb_vector_assign_string_element_len(output, row, resultStr.c_str(),
                                                resultStr.size());
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T, bool IsLng>
void CellToLatOrLngFunction(duckdb_function_info info, duckdb_data_chunk input,
                            duckdb_vector output) {
  static_assert(std::is_same<T, duckdb_string_t>::value ||
                    std::is_same<T, uint64_t>::value ||
                    std::is_same<T, int64_t>::value,
                "T must be an acceptable type");
  constexpr auto IsStringT = std::is_same<T, duckdb_string_t>::value;
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);

  duckdb_vector_ensure_validity_writable(output);
  double *resultData = (double *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    if (duckdb_validity_row_is_valid(indexVecValidity, row)) {
      H3Index cell;
      if constexpr (IsStringT) {
        auto str = static_cast<duckdb_string_t *>(&indexVecData[row]);
        auto strStr = DuckdbToString(str);
        H3Error err = stringToH3(strStr.c_str(), &cell);
        if (err) {
          cell = 0;
        }
      } else {
        cell = indexVecData[row];
      }

      if (cell) {
        LatLng latLng;
        H3Error err = cellToLatLng(cell, &latLng);
        if (!err) {
          resultData[row] = radsToDegs(IsLng ? latLng.lng : latLng.lat);
          wasValid = true;
        }
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void CellToLatLngFunction(duckdb_function_info info, duckdb_data_chunk input,
                          duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);

  duckdb_list_vector_reserve(output, inputSize * 2);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  double *resultData = (double *)duckdb_vector_get_data(outputChildVec);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    if (duckdb_validity_row_is_valid(indexVecValidity, row)) {
      H3Index cell = IndexFromVector(indexVecData, row);

      if (cell) {
        LatLng latLng;
        H3Error err = cellToLatLng(cell, &latLng);
        if (!err) {
          resultData[resultOffset] = radsToDegs(latLng.lat);
          resultData[resultOffset + 1] = radsToDegs(latLng.lng);
          entries[row].offset = resultOffset;
          entries[row].length = 2;
          resultOffset += 2;
          wasValid = true;
        }
      }
    }

    if (!wasValid) {
      entries[row].offset = resultOffset;
      entries[row].length = 0;
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

template <typename T, typename Encoder>
void CellToBoundaryFunction(duckdb_function_info info, duckdb_data_chunk input,
                            duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);

  duckdb_vector_ensure_validity_writable(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    if (duckdb_validity_row_is_valid(indexVecValidity, row)) {
      H3Index cell = IndexFromVector(indexVecData, row);

      if (cell) {
        CellBoundary boundary;
        H3Error err = cellToBoundary(cell, &boundary);
        if (!err) {
          auto enc = Encoder();
          enc.StartPolygon();
          for (int i = 0; i <= boundary.numVerts; i++) {
            // Add an extra vertex onto the end to close the polygon
            int vertIndex = (i == boundary.numVerts) ? 0 : i;
            enc.Point(radsToDegs(boundary.verts[vertIndex].lng),
                      radsToDegs(boundary.verts[vertIndex].lat));
          }
          enc.EndPolygon();
          auto str = enc.Finish();

          duckdb_vector_assign_string_element_len(output, row, str.c_str(),
                                                  str.size());
          wasValid = true;
        }
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

duckdb_scalar_function H3Functions::GetLatLngToCellFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_latlng_to_cell");
  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, ubigintType);
  duckdb_destroy_logical_type(&doubleType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&ubigintType);
  duckdb_scalar_function_set_function(function, LatLngToCellFunction);
  return function;
}

duckdb_scalar_function H3Functions::GetLatLngToCellVarcharFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_latlng_to_cell_string");
  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type stringType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, stringType);
  duckdb_destroy_logical_type(&doubleType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&stringType);
  duckdb_scalar_function_set_function(function, LatLngToCellVarcharFunction);
  return function;
}

duckdb_scalar_function_set H3Functions::GetCellToLatFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cell_to_lat");

  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);

  auto r = [&functionSet,
            &doubleType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_lat");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, doubleType);
    duckdb_scalar_function_set_function(
        function, CellToLatOrLngFunction<PhysicalType, false>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&doubleType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCellToLngFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cell_to_lng");

  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);

  auto r = [&functionSet,
            &doubleType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_lng");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, doubleType);
    duckdb_scalar_function_set_function(
        function, CellToLatOrLngFunction<PhysicalType, true>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&doubleType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCellToLatLngFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cell_to_latlng");

  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
  duckdb_logical_type doubleListType = duckdb_create_list_type(doubleType);

  auto r = [&functionSet,
            &doubleListType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_latlng");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, doubleListType);
    duckdb_scalar_function_set_function(function,
                                        CellToLatLngFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&doubleListType);
  duckdb_destroy_logical_type(&doubleType);

  return functionSet;
}

template <typename Encoder>
duckdb_scalar_function_set
GetCellToBoundaryGenericFunction(const char *name, duckdb_type returnTypeId) {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set(name);

  duckdb_logical_type returnType = duckdb_create_logical_type(returnTypeId);

  auto r = [&functionSet, &name,
            &returnType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, name);
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, returnType);
    duckdb_scalar_function_set_function(
        function, CellToBoundaryFunction<PhysicalType, Encoder>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.template operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.template operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.template operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&returnType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCellToBoundaryWktFunction() {
  return GetCellToBoundaryGenericFunction<WktEncoder>("h3_cell_to_boundary_wkt",
                                                      DUCKDB_TYPE_VARCHAR);
}

duckdb_scalar_function_set H3Functions::GetCellToBoundaryWkbFunction() {
  return GetCellToBoundaryGenericFunction<WkbEncoder>("h3_cell_to_boundary_wkb",
                                                      DUCKDB_TYPE_BLOB);
}

} // namespace h3duckdb
