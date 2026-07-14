#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace h3duckdb {

template <typename T>
void CellToVertexFunction(duckdb_function_info info, duckdb_data_chunk input,
                          duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector vertexVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *vertexVecData = (int32_t *)duckdb_vector_get_data(vertexVec);

  duckdb_vector_ensure_validity_writable(output);
  T *resultData = (T *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index index = IndexFromVector(indexVecData, row);
    auto vertex = vertexVecData[row];
    H3Index out;

    H3Error err = cellToVertex(index, vertex, &out);
    if (!err) {
      AssignHexString(output, resultData, row, out);
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void CellToVertexesFunction(duckdb_function_info info, duckdb_data_chunk input,
                            duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);

  // Worst case: all hexagons, so all 6 verts
  duckdb_list_vector_reserve(output, inputSize * 6);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  T *resultData = (T *)duckdb_vector_get_data(outputChildVec);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    H3Index cell = IndexFromVector(indexVecData, row);

    if (cell) {
      std::vector<H3Index> out(6);
      H3Error err = cellToVertexes(cell, out.data());
      if (!err) {
        idx_t actualCount = 0;
        for (idx_t j = 0; j < out.size(); ++j) {
          if (out[j]) {
            AssignHexString(outputChildVec, resultData,
                            resultOffset + actualCount, out[j]);
            actualCount++;
          }
        }

        entries[row].offset = resultOffset;
        entries[row].length = actualCount;
        resultOffset += actualCount;
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T, bool IsLng>
void VertexToLatOrLngFunction(duckdb_function_info info,
                              duckdb_data_chunk input, duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector vertexVec = duckdb_data_chunk_get_vector(input, 0);
  T *vertexVecData = (T *)duckdb_vector_get_data(vertexVec);

  duckdb_vector_ensure_validity_writable(output);
  double *resultData = (double *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index index = IndexFromVector(vertexVecData, row);

    LatLng out = {0};
    H3Error err = vertexToLatLng(index, &out);
    if (!err) {
      resultData[row] = radsToDegs(IsLng ? out.lng : out.lat);
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void VertexToLatLngFunction(duckdb_function_info info, duckdb_data_chunk input,
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
        H3Error err = vertexToLatLng(cell, &latLng);
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
}

struct IsValidVertexOperator {
  static bool operate(H3Index index) { return isValidVertex(index); }
};

duckdb_scalar_function_set H3Functions::GetCellToVertexFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cell_to_vertex");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_vertex");
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, bigintType);
    duckdb_scalar_function_set_function(function,
                                        CellToVertexFunction<int64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_vertex");
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, ubigintType);
    duckdb_scalar_function_set_function(function,
                                        CellToVertexFunction<uint64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_vertex");
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, varcharType);
    duckdb_scalar_function_set_function(function,
                                        CellToVertexFunction<duckdb_string_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCellToVertexesFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cell_to_vertexes");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type varcharListType = duckdb_create_list_type(varcharType);
  duckdb_logical_type ubigintListType = duckdb_create_list_type(ubigintType);
  duckdb_logical_type bigintListType = duckdb_create_list_type(bigintType);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_vertexes");
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_set_return_type(function, bigintListType);
    duckdb_scalar_function_set_function(function,
                                        CellToVertexesFunction<int64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_vertexes");
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_set_return_type(function, ubigintListType);
    duckdb_scalar_function_set_function(function,
                                        CellToVertexesFunction<uint64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_vertexes");
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_set_return_type(function, varcharListType);
    duckdb_scalar_function_set_function(
        function, CellToVertexesFunction<duckdb_string_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);
  duckdb_destroy_logical_type(&ubigintListType);
  duckdb_destroy_logical_type(&bigintListType);
  duckdb_destroy_logical_type(&varcharListType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetVertexToLatFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_vertex_to_lat");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_vertex_to_lat");
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_set_return_type(function, doubleType);
    duckdb_scalar_function_set_function(
        function, VertexToLatOrLngFunction<int64_t, false>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_vertex_to_lat");
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_set_return_type(function, doubleType);
    duckdb_scalar_function_set_function(
        function, VertexToLatOrLngFunction<uint64_t, false>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_vertex_to_lat");
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_set_return_type(function, doubleType);
    duckdb_scalar_function_set_function(
        function, VertexToLatOrLngFunction<duckdb_string_t, false>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&doubleType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetVertexToLngFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_vertex_to_lng");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_vertex_to_lng");
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_set_return_type(function, doubleType);
    duckdb_scalar_function_set_function(
        function, VertexToLatOrLngFunction<int64_t, true>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_vertex_to_lng");
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_set_return_type(function, doubleType);
    duckdb_scalar_function_set_function(
        function, VertexToLatOrLngFunction<uint64_t, true>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_vertex_to_lng");
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_set_return_type(function, doubleType);
    duckdb_scalar_function_set_function(
        function, VertexToLatOrLngFunction<duckdb_string_t, true>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&doubleType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetVertexToLatLngFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_vertex_to_Latlng");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
  duckdb_logical_type doubleListType = duckdb_create_list_type(doubleType);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_vertex_to_latlng");
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_set_return_type(function, doubleListType);
    duckdb_scalar_function_set_function(function,
                                        VertexToLatLngFunction<int64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_vertex_to_latlng");
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_set_return_type(function, doubleListType);
    duckdb_scalar_function_set_function(function,
                                        VertexToLatLngFunction<uint64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_vertex_to_latlng");
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_set_return_type(function, doubleListType);
    duckdb_scalar_function_set_function(
        function, VertexToLatLngFunction<duckdb_string_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&doubleType);
  duckdb_destroy_logical_type(&doubleListType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetIsValidVertexFunctions() {
  return GetGenericInspectFunction<bool, IsValidVertexOperator>(
      "h3_is_valid_vertex", DUCKDB_TYPE_BOOLEAN);
}

} // namespace h3duckdb
