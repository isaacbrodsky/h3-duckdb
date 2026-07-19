#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace h3duckdb {

struct GridDiskOperator {
  static H3Error fn(H3Index origin, int32_t k, H3Index *out) {
    return gridDisk(origin, k, out);
  }
};

struct GridDiskUnsafeOperator {
  static H3Error fn(H3Index origin, int32_t k, H3Index *out) {
    return gridDiskUnsafe(origin, k, out);
  }
};

struct GridDiskDistancesOperator {
  static H3Error fn(H3Index origin, int32_t k, H3Index *out,
                    int32_t *distancesOut) {
    return gridDiskDistances(origin, k, out, distancesOut);
  }
};

struct GridDiskDistancesSafeOperator {
  static H3Error fn(H3Index origin, int32_t k, H3Index *out,
                    int32_t *distancesOut) {
    return gridDiskDistancesSafe(origin, k, out, distancesOut);
  }
};

struct GridDiskDistancesUnsafeOperator {
  static H3Error fn(H3Index origin, int32_t k, H3Index *out,
                    int32_t *distancesOut) {
    return gridDiskDistancesUnsafe(origin, k, out, distancesOut);
  }
};

template <typename T, class Operator>
void GridDiskGenericFunction(duckdb_function_info info, duckdb_data_chunk input,
                             duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector kVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *kVecData = (int32_t *)duckdb_vector_get_data(kVec);

  idx_t totalSize = 0;
  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t currentOut = 0;
      H3Error err = maxGridDiskSize(k, &currentOut);

      if (!err) {
        totalSize += currentOut;
      }
    }
  }

  duckdb_list_vector_reserve(output, totalSize);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  T *resultData = (T *)duckdb_vector_get_data(outputChildVec);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t sz;
      H3Error err1 = maxGridDiskSize(k, &sz);
      if (!err1) {
        std::vector<H3Index> out(sz);
        H3Error err2 = Operator::fn(parent, k, out.data());
        if (!err2) {
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
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

template <typename T, bool Unsafe>
void GridRingFunction(duckdb_function_info info, duckdb_data_chunk input,
                      duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector kVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *kVecData = (int32_t *)duckdb_vector_get_data(kVec);

  idx_t totalSize = 0;
  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t currentOut = 0;
      H3Error err = maxGridRingSize(k, &currentOut);

      if (!err) {
        totalSize += currentOut;
      }
    }
  }

  duckdb_list_vector_reserve(output, totalSize);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  T *resultData = (T *)duckdb_vector_get_data(outputChildVec);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t sz;
      H3Error err1 = maxGridRingSize(k, &sz);
      if (!err1) {
        std::vector<H3Index> out(sz);
        H3Error err2 = Unsafe ? gridRingUnsafe(parent, k, out.data())
                              : gridRing(parent, k, out.data());
        if (!err2) {
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
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

template <typename T, class Operator>
void GridDiskDistancesGenericFunction(duckdb_function_info info,
                                      duckdb_data_chunk input,
                                      duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector kVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *kVecData = (int32_t *)duckdb_vector_get_data(kVec);

  idx_t totalSize = 0;
  idx_t kSize = inputSize;
  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t currentOut = 0;
      H3Error err = maxGridDiskSize(k, &currentOut);

      if (!err) {
        totalSize += currentOut;
        kSize += k;
      }
    }
  }

  duckdb_list_vector_reserve(output, kSize);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  duckdb_list_vector_reserve(outputChildVec, totalSize);
  duckdb_list_entry *resultData =
      (duckdb_list_entry *)duckdb_vector_get_data(outputChildVec);
  duckdb_vector outputChild2Vec = duckdb_list_vector_get_child(outputChildVec);
  T *result2Data = (T *)duckdb_vector_get_data(outputChild2Vec);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;
  idx_t entriesOffset = 0;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t sz;
      H3Error err1 = maxGridDiskSize(k, &sz);
      if (!err1) {
        std::vector<H3Index> out(sz);
        std::vector<int32_t> distances(sz);
        H3Error err2 = Operator::fn(parent, k, out.data(), distances.data());
        if (!err2) {
          for (idx_t dist = 0; dist <= k; ++dist) {
            idx_t actualCount = 0;

            for (idx_t j = 0; j < out.size(); ++j) {
              if (out[j] && distances[j] == dist) {
                AssignHexString(outputChild2Vec, result2Data,
                                resultOffset + actualCount, out[j]);
                actualCount++;
              }
            }
            resultData[row + dist].offset = resultOffset;
            resultData[row + dist].length = actualCount;

            resultOffset += actualCount;
          }

          entries[row].offset = entriesOffset;
          entries[row].length = k + 1;
          wasValid = true;
          entriesOffset += k + 1;
        }
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(outputChildVec, resultOffset);
  duckdb_list_vector_set_size(output, entriesOffset);
}

template <typename T>
void GridPathCellsFunction(duckdb_function_info info, duckdb_data_chunk input,
                           duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector index2Vec = duckdb_data_chunk_get_vector(input, 1);
  T *index2VecData = (T *)duckdb_vector_get_data(index2Vec);

  idx_t totalSize = 0;
  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index index0 = IndexFromVector(indexVecData, row);
    H3Index index1 = IndexFromVector(index2VecData, row);

    if (index0 && index1) {
      int64_t currentOut = 0;
      H3Error err = gridPathCellsSize(index0, index1, &currentOut);

      if (!err) {
        totalSize += currentOut;
      }
    }
  }

  duckdb_list_vector_reserve(output, totalSize);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  T *resultData = (T *)duckdb_vector_get_data(outputChildVec);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    H3Index index0 = IndexFromVector(indexVecData, row);
    H3Index index1 = IndexFromVector(index2VecData, row);

    if (index0 && index1) {
      int64_t sz;
      H3Error err1 = gridPathCellsSize(index0, index1, &sz);
      if (!err1) {
        std::vector<H3Index> out(sz);
        H3Error err2 = gridPathCells(index0, index1, out.data());
        if (!err2) {
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
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

template <typename T>
void GridDistanceFunction(duckdb_function_info info, duckdb_data_chunk input,
                          duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector index2Vec = duckdb_data_chunk_get_vector(input, 1);
  T *index2VecData = (T *)duckdb_vector_get_data(index2Vec);

  duckdb_vector_ensure_validity_writable(output);
  int64_t *resultData = (int64_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    H3Index index0 = IndexFromVector(indexVecData, row);
    H3Index index1 = IndexFromVector(index2VecData, row);

    if (index0 && index1) {
      int64_t sz;
      H3Error err1 = gridDistance(index0, index1, &sz);
      if (!err1) {
        resultData[row] = sz;
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void CellToLocalIjFunction(duckdb_function_info info, duckdb_data_chunk input,
                           duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector index2Vec = duckdb_data_chunk_get_vector(input, 1);
  T *index2VecData = (T *)duckdb_vector_get_data(index2Vec);

  duckdb_list_vector_reserve(output, inputSize * 2);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  int32_t *resultData = (int32_t *)duckdb_vector_get_data(outputChildVec);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    H3Index index0 = IndexFromVector(indexVecData, row);
    H3Index index1 = IndexFromVector(index2VecData, row);

    if (index0 && index1) {
      CoordIJ ij;
      int32_t mode = 0;
      H3Error err = cellToLocalIj(index0, index1, mode, &ij);
      if (!err) {
        resultData[resultOffset] = ij.i;
        resultData[resultOffset + 1] = ij.j;

        entries[row].offset = resultOffset;
        entries[row].length = 2;
        resultOffset += 2;
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

template <typename T>
void LocalIjToCellFunction(duckdb_function_info info, duckdb_data_chunk input,
                           duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector iVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *iVecData = (int32_t *)duckdb_vector_get_data(iVec);
  duckdb_vector jVec = duckdb_data_chunk_get_vector(input, 2);
  int32_t *jVecData = (int32_t *)duckdb_vector_get_data(jVec);

  duckdb_vector_ensure_validity_writable(output);
  T *resultData = (T *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    H3Index index = IndexFromVector(indexVecData, row);
    auto i = iVecData[row];
    auto j = jVecData[row];

    if (index) {
      int32_t mode = 0;
      CoordIJ ij = {.i = i, .j = j};
      H3Index result;
      H3Error err = localIjToCell(index, &ij, mode, &result);
      if (!err) {
        AssignHexString(output, resultData, row, result);
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

void MaxGridDiskSizeFunction(duckdb_function_info info, duckdb_data_chunk input,
                             duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector kVec = duckdb_data_chunk_get_vector(input, 0);
  int32_t *kVecData = (int32_t *)duckdb_vector_get_data(kVec);

  duckdb_vector_ensure_validity_writable(output);
  int64_t *resultData = (int64_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    auto k = kVecData[row];
    int64_t out = 0;

    H3Error err = maxGridDiskSize(k, &out);
    if (!err) {
      resultData[row] = out;
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

duckdb_scalar_function_set H3Functions::GetGridDiskFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_disk");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_disk");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalListType);
    duckdb_scalar_function_set_function(
        function, GridDiskGenericFunction<PhysicalType, GridDiskOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&logicalListType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function H3Functions::GetMaxGridDiskSizeFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_max_grid_disk_size");
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, bigintType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_scalar_function_set_function(function, MaxGridDiskSizeFunction);
  return function;
}

duckdb_scalar_function_set H3Functions::GetGridDiskDistancesFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_disk_distances");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);
    auto logicalListListType = duckdb_create_list_type(logicalListType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_disk_distances");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalListListType);
    duckdb_scalar_function_set_function(
        function, GridDiskDistancesGenericFunction<PhysicalType,
                                                   GridDiskDistancesOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&logicalListType);
    duckdb_destroy_logical_type(&logicalListListType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetGridDiskDistancesUnsafeFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_disk_distances_unsafe");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);
    auto logicalListListType = duckdb_create_list_type(logicalListType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_disk_distances_unsafe");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalListListType);
    duckdb_scalar_function_set_function(
        function,
        GridDiskDistancesGenericFunction<PhysicalType,
                                         GridDiskDistancesUnsafeOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&logicalListType);
    duckdb_destroy_logical_type(&logicalListListType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetGridDiskDistancesSafeFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_disk_distances_safe");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);
    auto logicalListListType = duckdb_create_list_type(logicalListType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_disk_distances_safe");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalListListType);
    duckdb_scalar_function_set_function(
        function,
        GridDiskDistancesGenericFunction<PhysicalType,
                                         GridDiskDistancesSafeOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&logicalListType);
    duckdb_destroy_logical_type(&logicalListListType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetGridDiskUnsafeFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_disk_unsafe");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_disk_unsafe");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalListType);
    duckdb_scalar_function_set_function(
        function,
        GridDiskGenericFunction<PhysicalType, GridDiskUnsafeOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&logicalListType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetGridRingFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_ring");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_ring");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalListType);
    duckdb_scalar_function_set_function(function,
                                        GridRingFunction<PhysicalType, false>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&logicalListType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetGridRingUnsafeFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_ring_unsafe");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_ring_unsafe");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalListType);
    duckdb_scalar_function_set_function(function,
                                        GridRingFunction<PhysicalType, true>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&logicalListType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetGridPathCellsFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_path_cells");

  auto r = [&functionSet]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_path_cells");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, logicalListType);
    duckdb_scalar_function_set_function(function,
                                        GridPathCellsFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&logicalListType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetGridDistanceFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_distance");

  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);

  auto r = [&functionSet,
            &bigintType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_distance");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, bigintType);
    duckdb_scalar_function_set_function(function,
                                        GridDistanceFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&bigintType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCellToLocalIjFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cell_to_local_ij");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type intListType = duckdb_create_list_type(intType);

  auto r = [&functionSet,
            &intListType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_local_ij");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, intListType);
    duckdb_scalar_function_set_function(function,
                                        CellToLocalIjFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&intListType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetLocalIjToCellFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_local_ij_to_cell");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_local_ij_to_cell");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalType);
    duckdb_scalar_function_set_function(function,
                                        LocalIjToCellFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

} // namespace h3duckdb
