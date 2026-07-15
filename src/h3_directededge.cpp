#include "h3_common.hpp"
#include "h3_functions.hpp"
#include "well_known_encoder.hpp"

namespace h3duckdb {

template <typename T>
void DirectedEdgeToCellsFunction(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);

  duckdb_list_vector_reserve(output, inputSize * 2);
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
      std::vector<H3Index> out(2);
      H3Error err = directedEdgeToCells(cell, out.data());
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

template <typename T>
void OriginToDirectedEdgesFunction(duckdb_function_info info,
                                   duckdb_data_chunk input,
                                   duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);

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
      H3Error err = originToDirectedEdges(cell, out.data());
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

template <typename T, bool IsDestination>
void GetDirectedEdgeOriginOrDestinationFunction(duckdb_function_info info,
                                                duckdb_data_chunk input,
                                                duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);

  duckdb_vector_ensure_validity_writable(output);
  T *resultData = (T *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index index = IndexFromVector(indexVecData, row);
    H3Index out;

    H3Error err = IsDestination ? getDirectedEdgeDestination(index, &out)
                                : getDirectedEdgeOrigin(index, &out);
    if (!err) {
      AssignHexString(output, resultData, row, out);
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void CellsToDirectedEdgeFunction(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector index2Vec = duckdb_data_chunk_get_vector(input, 1);
  T *index2VecData = (T *)duckdb_vector_get_data(index2Vec);

  duckdb_vector_ensure_validity_writable(output);
  T *resultData = (T *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index index = IndexFromVector(indexVecData, row);
    H3Index index2 = IndexFromVector(index2VecData, row);
    H3Index out;

    H3Error err = cellsToDirectedEdge(index, index2, &out);
    if (!err) {
      AssignHexString(output, resultData, row, out);
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void ReverseDirectedEdgeFunction(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);

  duckdb_vector_ensure_validity_writable(output);
  T *resultData = (T *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index index = IndexFromVector(indexVecData, row);
    H3Index out;

    H3Error err = reverseDirectedEdge(index, &out);
    if (!err) {
      AssignHexString(output, resultData, row, out);
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void AreNeighborCellsFunction(duckdb_function_info info,
                              duckdb_data_chunk input, duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector index2Vec = duckdb_data_chunk_get_vector(input, 1);
  T *index2VecData = (T *)duckdb_vector_get_data(index2Vec);

  duckdb_vector_ensure_validity_writable(output);
  bool *resultData = (bool *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index index = IndexFromVector(indexVecData, row);
    H3Index index2 = IndexFromVector(index2VecData, row);

    int out;
    H3Error err = areNeighborCells(index, index2, &out);
    if (!err) {
      resultData[row] = out;
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

struct IsValidDirectedEdgeOperator {
  static bool operate(H3Index index) { return isValidDirectedEdge(index); }
};

template <typename T, typename Encoder>
void DirectedEdgeToBoundaryFunction(duckdb_function_info info,
                                    duckdb_data_chunk input,
                                    duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);

  duckdb_vector_ensure_validity_writable(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    H3Index cell = IndexFromVector(indexVecData, row);

    if (cell) {
      CellBoundary boundary;
      H3Error err = directedEdgeToBoundary(cell, &boundary);
      if (!err) {
        auto enc = Encoder();
        enc.StartLineString();
        for (int i = 0; i <= boundary.numVerts; i++) {
          // Add an extra vertex onto the end to close the polygon
          int vertIndex = (i == boundary.numVerts) ? 0 : i;
          enc.Point(radsToDegs(boundary.verts[vertIndex].lng),
                    radsToDegs(boundary.verts[vertIndex].lat));
        }
        enc.EndLineString();
        auto str = enc.Finish();

        duckdb_vector_assign_string_element_len(output, row, str.c_str(),
                                                str.size());
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

duckdb_scalar_function_set H3Functions::GetDirectedEdgeToCellsFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_directed_edge_to_cells");

  auto r = [&functionSet]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_logical_type returnType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_directed_edge_to_cells");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, returnType);
    duckdb_scalar_function_set_function(
        function, DirectedEdgeToCellsFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&returnType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetOriginToDirectedEdgesFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_origin_to_directed_edges");

  auto r = [&functionSet]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_logical_type returnType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_origin_to_directed_edges");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, returnType);
    duckdb_scalar_function_set_function(
        function, OriginToDirectedEdgesFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&returnType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetGetDirectedEdgeOriginFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_get_directed_edge_origin");

  auto r = [&functionSet]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_get_directed_edge_origin");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, logicalType);
    duckdb_scalar_function_set_function(
        function,
        GetDirectedEdgeOriginOrDestinationFunction<PhysicalType, false>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  return functionSet;
}

duckdb_scalar_function_set
H3Functions::GetGetDirectedEdgeDestinationFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_get_directed_edge_destination");

  auto r = [&functionSet]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function,
                                    "h3_get_directed_edge_destination");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, logicalType);
    duckdb_scalar_function_set_function(
        function,
        GetDirectedEdgeOriginOrDestinationFunction<PhysicalType, true>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCellsToDirectedEdgeFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cells_to_directed_edge");

  auto r = [&functionSet]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cells_to_directed_edge");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, logicalType);
    duckdb_scalar_function_set_function(
        function, CellsToDirectedEdgeFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetAreNeighborCellsFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_are_neighbor_cells");

  duckdb_logical_type boolType =
      duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);

  auto r = [&functionSet,
            &boolType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_are_neighbor_cells");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, boolType);
    duckdb_scalar_function_set_function(function,
                                        AreNeighborCellsFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&boolType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetIsValidDirectedEdgeFunctions() {
  return GetGenericInspectFunction<bool, IsValidDirectedEdgeOperator>(
      "h3_is_valid_directed_edge", DUCKDB_TYPE_BOOLEAN);
}

template <typename Encoder>
duckdb_scalar_function_set
GetDirectedEdgeToBoundaryGenericFunction(const char *name,
                                         duckdb_type returnTypeId) {
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
        function, DirectedEdgeToBoundaryFunction<PhysicalType, Encoder>);
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

duckdb_scalar_function_set H3Functions::GetDirectedEdgeToBoundaryWktFunction() {
  return GetDirectedEdgeToBoundaryGenericFunction<WktEncoder>(
      "h3_directed_edge_to_boundary_wkt", DUCKDB_TYPE_VARCHAR);
}

duckdb_scalar_function_set H3Functions::GetDirectedEdgeToBoundaryWkbFunction() {
  return GetDirectedEdgeToBoundaryGenericFunction<WkbEncoder>(
      "h3_directed_edge_to_boundary_wkb", DUCKDB_TYPE_BLOB);
}

duckdb_scalar_function_set H3Functions::GetReverseDirectedEdgeFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_reverse_directed_edge");

  auto r = [&functionSet]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_reverse_directed_edge");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, logicalType);
    duckdb_scalar_function_set_function(
        function, ReverseDirectedEdgeFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  return functionSet;
}

} // namespace h3duckdb
