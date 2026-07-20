#include "h3_functions.hpp"

namespace h3duckdb {

// TODO: Consider using enums for (km, m, rads) here, instead of VARCHAR
// (string)
// TODO: Or separate functions

void GetHexagonAreaAvgFunction(duckdb_function_info info,
                               duckdb_data_chunk input, duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 0);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);
  duckdb_vector unitVec = duckdb_data_chunk_get_vector(input, 1);
  duckdb_string_t *unitVecData =
      (duckdb_string_t *)duckdb_vector_get_data(unitVec);
  uint64_t *unitVecValidity = duckdb_vector_get_validity(unitVec);

  duckdb_vector_ensure_validity_writable(output);
  double *resultData = (double *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    if (duckdb_validity_row_is_valid(resVecValidity, row) &&
        duckdb_validity_row_is_valid(unitVecValidity, row)) {
      int res = resVecData[row];
      auto unit = &unitVecData[row];

      double out;
      H3Error err = E_OPTION_INVALID;
      auto unitStr = DuckdbToString(unit);
      if (unitStr == "km^2") {
        err = getHexagonAreaAvgKm2(res, &out);
      } else if (unitStr == "m^2") {
        err = getHexagonAreaAvgM2(res, &out);
      }

      if (!err) {
        resultData[row] = out;
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T> struct CellAreaOperator {
  static void operate(duckdb_function_info info, duckdb_data_chunk input,
                      duckdb_vector output) {
    idx_t inputSize = duckdb_data_chunk_get_size(input);

    duckdb_vector cellVec = duckdb_data_chunk_get_vector(input, 0);
    T *cellVecData = (T *)duckdb_vector_get_data(cellVec);
    uint64_t *cellVecValidity = duckdb_vector_get_validity(cellVec);
    duckdb_vector unitVec = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *unitVecData =
        (duckdb_string_t *)duckdb_vector_get_data(unitVec);
    uint64_t *unitVecValidity = duckdb_vector_get_validity(unitVec);

    duckdb_vector_ensure_validity_writable(output);
    double *resultData = (double *)duckdb_vector_get_data(output);
    uint64_t *resultValidity = duckdb_vector_get_validity(output);

    for (idx_t row = 0; row < inputSize; ++row) {
      bool wasValid = false;
      if (duckdb_validity_row_is_valid(cellVecValidity, row) &&
          duckdb_validity_row_is_valid(unitVecValidity, row)) {
        H3Index cell = IndexFromVector(cellVecData, row);
        auto unit = &unitVecData[row];

        double out;
        H3Error err = E_OPTION_INVALID;
        auto unitStr = DuckdbToString(unit);
        if (unitStr == "rads^2") {
          err = cellAreaRads2(cell, &out);
        } else if (unitStr == "km^2") {
          err = cellAreaKm2(cell, &out);
        } else if (unitStr == "m^2") {
          err = cellAreaM2(cell, &out);
        }

        if (!err) {
          resultData[row] = out;
          wasValid = true;
        }
      }

      if (!wasValid) {
        duckdb_validity_set_row_invalid(resultValidity, row);
      }
    }
  }
};

void GetHexagonEdgeLengthAvgFunction(duckdb_function_info info,
                                     duckdb_data_chunk input,
                                     duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 0);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);
  duckdb_vector unitVec = duckdb_data_chunk_get_vector(input, 1);
  duckdb_string_t *unitVecData =
      (duckdb_string_t *)duckdb_vector_get_data(unitVec);
  uint64_t *unitVecValidity = duckdb_vector_get_validity(unitVec);

  duckdb_vector_ensure_validity_writable(output);
  double *resultData = (double *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    if (duckdb_validity_row_is_valid(resVecValidity, row) &&
        duckdb_validity_row_is_valid(unitVecValidity, row)) {
      int res = resVecData[row];
      auto unit = &unitVecData[row];

      double out;
      H3Error err = E_OPTION_INVALID;
      auto unitStr = DuckdbToString(unit);
      if (unitStr == "km") {
        err = getHexagonEdgeLengthAvgKm(res, &out);
      } else if (unitStr == "m") {
        err = getHexagonEdgeLengthAvgM(res, &out);
      }

      if (!err) {
        resultData[row] = out;
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T> struct EdgeLengthOperator {
  static void operate(duckdb_function_info info, duckdb_data_chunk input,
                      duckdb_vector output) {
    idx_t inputSize = duckdb_data_chunk_get_size(input);

    duckdb_vector cellVec = duckdb_data_chunk_get_vector(input, 0);
    T *cellVecData = (T *)duckdb_vector_get_data(cellVec);
    uint64_t *cellVecValidity = duckdb_vector_get_validity(cellVec);
    duckdb_vector unitVec = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *unitVecData =
        (duckdb_string_t *)duckdb_vector_get_data(unitVec);
    uint64_t *unitVecValidity = duckdb_vector_get_validity(unitVec);

    duckdb_vector_ensure_validity_writable(output);
    double *resultData = (double *)duckdb_vector_get_data(output);
    uint64_t *resultValidity = duckdb_vector_get_validity(output);

    for (idx_t row = 0; row < inputSize; ++row) {
      bool wasValid = false;
      if (duckdb_validity_row_is_valid(cellVecValidity, row) &&
          duckdb_validity_row_is_valid(unitVecValidity, row)) {
        H3Index edge = IndexFromVector(cellVecData, row);
        auto unit = &unitVecData[row];

        double out;
        H3Error err = E_OPTION_INVALID;
        auto unitStr = DuckdbToString(unit);
        if (unitStr == "rads") {
          err = edgeLengthRads(edge, &out);
        } else if (unitStr == "km") {
          err = edgeLengthKm(edge, &out);
        } else if (unitStr == "m") {
          err = edgeLengthM(edge, &out);
        }

        if (!err) {
          resultData[row] = out;
          wasValid = true;
        }
      }

      if (!wasValid) {
        duckdb_validity_set_row_invalid(resultValidity, row);
      }
    }
  }
};

void GetNumCellsFunction(duckdb_function_info info, duckdb_data_chunk input,
                         duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 0);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_vector_ensure_validity_writable(output);
  int64_t *resultData = (int64_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    if (duckdb_validity_row_is_valid(resVecValidity, row)) {
      int res = resVecData[row];

      int64_t out;
      H3Error err = getNumCells(res, &out);

      if (!err) {
        resultData[row] = out;
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void GetRes0CellsFunction(duckdb_function_info info, duckdb_data_chunk input,
                          duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  int sz = res0CellCount();

  duckdb_list_vector_reserve(output, inputSize * sz);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  T *resultData = (T *)duckdb_vector_get_data(outputChildVec);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;

  std::vector<H3Index> out(sz);
  H3Error err = getRes0Cells(out.data());

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    if (!err) {
      for (idx_t i = 0; i < sz; ++i) {
        AssignHexString(outputChildVec, resultData, resultOffset + i, out[i]);
      }
      entries[row].offset = resultOffset;
      entries[row].length = sz;
      resultOffset += sz;
      wasValid = true;
    }

    if (!wasValid) {
      // TODO: This should be unreachable
      entries[row].offset = resultOffset;
      entries[row].length = 0;
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

template <typename T>
void GetPentagonsFunction(duckdb_function_info info, duckdb_data_chunk input,
                          duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  int sz = pentagonCount();

  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 0);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_list_vector_reserve(output, inputSize * sz);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  T *resultData = (T *)duckdb_vector_get_data(outputChildVec);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    if (duckdb_validity_row_is_valid(resVecValidity, row)) {
      auto res = resVecData[row];

      std::vector<H3Index> out(sz);
      H3Error err = getPentagons(res, out.data());

      if (!err) {
        for (idx_t i = 0; i < sz; ++i) {
          AssignHexString(outputChildVec, resultData, resultOffset + i, out[i]);
        }
        entries[row].offset = resultOffset;
        entries[row].length = sz;
        resultOffset += sz;
        wasValid = true;
      }
    }

    if (!wasValid) {
      // TODO: This should be unreachable
      entries[row].offset = resultOffset;
      entries[row].length = 0;
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

void GetPentagonsVarcharFunction(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  int sz = pentagonCount();

  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 0);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_list_vector_reserve(output, inputSize * sz);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    if (duckdb_validity_row_is_valid(resVecValidity, row)) {
      auto res = resVecData[row];

      std::vector<H3Index> out(sz);
      H3Error err = getPentagons(res, out.data());

      if (!err) {
        for (idx_t i = 0; i < sz; ++i) {
          std::string resultStr = ToHexString(out[i]);
          duckdb_vector_assign_string_element_len(
              outputChildVec, resultOffset + i, resultStr.c_str(),
              resultStr.size());
        }
        entries[row].offset = resultOffset;
        entries[row].length = sz;
        resultOffset += sz;
        wasValid = true;
      }
    }

    if (!wasValid) {
      // TODO: This should be unreachable
      entries[row].offset = resultOffset;
      entries[row].length = 0;
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

void GreatCircleDistanceFunction(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector lat0Vec = duckdb_data_chunk_get_vector(input, 0);
  double *lat0VecData = (double *)duckdb_vector_get_data(lat0Vec);
  uint64_t *lat0VecValidity = duckdb_vector_get_validity(lat0Vec);
  duckdb_vector lng0Vec = duckdb_data_chunk_get_vector(input, 1);
  double *lng0VecData = (double *)duckdb_vector_get_data(lng0Vec);
  uint64_t *lng0VecValidity = duckdb_vector_get_validity(lng0Vec);
  duckdb_vector lat1Vec = duckdb_data_chunk_get_vector(input, 2);
  double *lat1VecData = (double *)duckdb_vector_get_data(lat1Vec);
  uint64_t *lat1VecValidity = duckdb_vector_get_validity(lat1Vec);
  duckdb_vector lng1Vec = duckdb_data_chunk_get_vector(input, 3);
  double *lng1VecData = (double *)duckdb_vector_get_data(lng1Vec);
  uint64_t *lng1VecValidity = duckdb_vector_get_validity(lat1Vec);
  duckdb_vector unitVec = duckdb_data_chunk_get_vector(input, 4);
  duckdb_string_t *unitVecData =
      (duckdb_string_t *)duckdb_vector_get_data(unitVec);
  uint64_t *unitVecValidity = duckdb_vector_get_validity(unitVec);

  duckdb_vector_ensure_validity_writable(output);
  double *resultData = (double *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    if (duckdb_validity_row_is_valid(lat0VecValidity, row) &&
        duckdb_validity_row_is_valid(lng0VecValidity, row) &&
        duckdb_validity_row_is_valid(lat1VecValidity, row) &&
        duckdb_validity_row_is_valid(lng1VecValidity, row) &&
        duckdb_validity_row_is_valid(unitVecValidity, row)) {
      double lat0 = lat0VecData[row];
      double lng0 = lng0VecData[row];
      double lat1 = lat1VecData[row];
      double lng1 = lng1VecData[row];
      auto unit = &unitVecData[row];

      LatLng latLng0 = {.lat = degsToRads(lat0), .lng = degsToRads(lng0)};
      LatLng latLng1 = {.lat = degsToRads(lat1), .lng = degsToRads(lng1)};

      double out;
      auto unitStr = DuckdbToString(unit);
      if (unitStr == "rads") {
        resultData[row] = greatCircleDistanceRads(&latLng0, &latLng1);
        wasValid = true;
      } else if (unitStr == "km") {
        resultData[row] = greatCircleDistanceKm(&latLng0, &latLng1);
        wasValid = true;
      } else if (unitStr == "m") {
        resultData[row] = greatCircleDistanceM(&latLng0, &latLng1);
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

duckdb_scalar_function
GetGetHexagonGenericAvgFunction(const char *name, duckdb_scalar_function_t fn) {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, name);
  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_add_parameter(function, varcharType);
  duckdb_scalar_function_set_return_type(function, doubleType);
  duckdb_destroy_logical_type(&doubleType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_scalar_function_set_function(function, fn);
  return function;
}

template <template <typename> class Operator>
duckdb_scalar_function_set
GetCellAreaOrEdgeLengthGenericFunction(const char *name) {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set(name);

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);

  auto r = [&functionSet, &name, &varcharType,
            &doubleType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, name);
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_set_return_type(function, doubleType);
    duckdb_scalar_function_set_function(function,
                                        Operator<PhysicalType>::operate);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.template operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.template operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.template operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&doubleType);

  return functionSet;
}

duckdb_scalar_function H3Functions::GetGetHexagonAreaAvgFunction() {
  return GetGetHexagonGenericAvgFunction("h3_get_hexagon_area_avg",
                                         GetHexagonAreaAvgFunction);
}

duckdb_scalar_function_set H3Functions::GetCellAreaFunction() {
  return GetCellAreaOrEdgeLengthGenericFunction<CellAreaOperator>(
      "h3_cell_area");
}

duckdb_scalar_function H3Functions::GetGetHexagonEdgeLengthAvgFunction() {
  return GetGetHexagonGenericAvgFunction("h3_get_hexagon_edge_length_avg",
                                         GetHexagonEdgeLengthAvgFunction);
}

duckdb_scalar_function_set H3Functions::GetEdgeLengthFunction() {
  return GetCellAreaOrEdgeLengthGenericFunction<EdgeLengthOperator>(
      "h3_edge_length");
}

duckdb_scalar_function H3Functions::GetGetNumCellsFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_get_num_cells");
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, bigintType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&intType);
  duckdb_scalar_function_set_function(function, GetNumCellsFunction);
  return function;
}

duckdb_scalar_function H3Functions::GetGetRes0CellsFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_get_res0_cells");
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type ubigintListType = duckdb_create_list_type(ubigintType);
  duckdb_scalar_function_set_return_type(function, ubigintListType);
  duckdb_destroy_logical_type(&ubigintListType);
  duckdb_destroy_logical_type(&ubigintType);
  duckdb_scalar_function_set_function(function, GetRes0CellsFunction<uint64_t>);
  return function;
}

duckdb_scalar_function H3Functions::GetGetRes0CellsVarcharFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_get_res0_cells_string");
  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type varcharListType = duckdb_create_list_type(varcharType);
  duckdb_scalar_function_set_return_type(function, varcharListType);
  duckdb_destroy_logical_type(&varcharListType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_scalar_function_set_function(function,
                                      GetRes0CellsFunction<duckdb_string_t>);
  return function;
}

duckdb_scalar_function H3Functions::GetGetPentagonsFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_get_pentagons");
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type ubigintListType = duckdb_create_list_type(ubigintType);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, ubigintListType);
  duckdb_destroy_logical_type(&ubigintListType);
  duckdb_destroy_logical_type(&ubigintType);
  duckdb_scalar_function_set_function(function, GetPentagonsFunction<uint64_t>);
  return function;
}

duckdb_scalar_function H3Functions::GetGetPentagonsVarcharFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_get_pentagons_string");
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type varcharListType = duckdb_create_list_type(varcharType);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, varcharListType);
  duckdb_destroy_logical_type(&varcharListType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_scalar_function_set_function(function,
                                      GetPentagonsFunction<duckdb_string_t>);
  return function;
}

duckdb_scalar_function H3Functions::GetGreatCircleDistanceFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_great_circle_distance");
  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, varcharType);
  duckdb_scalar_function_set_return_type(function, doubleType);
  duckdb_destroy_logical_type(&doubleType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_scalar_function_set_function(function, GreatCircleDistanceFunction);
  return function;
}

} // namespace h3duckdb
