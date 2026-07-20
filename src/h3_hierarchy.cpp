#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace h3duckdb {

template <typename T>
void CellToParentFunction(duckdb_function_info info, duckdb_data_chunk input,
                          duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_vector_ensure_validity_writable(output);
  T *resultData = (T *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    if (duckdb_validity_row_is_valid(indexVecValidity, row) &&
        duckdb_validity_row_is_valid(resVecValidity, row)) {
      H3Index index = IndexFromVector(indexVecData, row);
      auto res = resVecData[row];
      H3Index out;

      H3Error err = cellToParent(index, res, &out);
      if (!err) {
        AssignHexString(output, resultData, row, out);
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void CellToChildrenFunction(duckdb_function_info info, duckdb_data_chunk input,
                            duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  idx_t totalSize = 0;
  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index parent = IndexFromVector(indexVecData, row);
    auto res = resVecData[row];

    if (duckdb_validity_row_is_valid(indexVecValidity, row) &&
        duckdb_validity_row_is_valid(resVecValidity, row) && parent) {
      int64_t currentOut = 0;
      H3Error err = cellToChildrenSize(parent, res, &currentOut);

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

    if (duckdb_validity_row_is_valid(indexVecValidity, row) &&
        duckdb_validity_row_is_valid(resVecValidity, row)) {
      H3Index parent = IndexFromVector(indexVecData, row);
      auto res = resVecData[row];

      if (parent) {
        int64_t sz;
        H3Error err1 = cellToChildrenSize(parent, res, &sz);
        if (!err1) {
          std::vector<H3Index> out(sz);
          H3Error err2 = cellToChildren(parent, res, out.data());
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
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

template <typename T>
void CellToChildrenSizeFunction(duckdb_function_info info,
                                duckdb_data_chunk input, duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_vector_ensure_validity_writable(output);
  int64_t *resultData = (int64_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    if (duckdb_validity_row_is_valid(indexVecValidity, row) &&
        duckdb_validity_row_is_valid(resVecValidity, row)) {
      H3Index index = IndexFromVector(indexVecData, row);
      auto res = resVecData[row];
      int64_t out;

      H3Error err = cellToChildrenSize(index, res, &out);
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
void CellToCenterChildFunction(duckdb_function_info info,
                               duckdb_data_chunk input, duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_vector_ensure_validity_writable(output);
  T *resultData = (T *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    if (duckdb_validity_row_is_valid(indexVecValidity, row) &&
        duckdb_validity_row_is_valid(resVecValidity, row)) {
      H3Index index = IndexFromVector(indexVecData, row);
      auto res = resVecData[row];
      H3Index out;

      H3Error err = cellToCenterChild(index, res, &out);
      if (!err) {
        AssignHexString(output, resultData, row, out);
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void CellToChildPosFunction(duckdb_function_info info, duckdb_data_chunk input,
                            duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_vector_ensure_validity_writable(output);
  int64_t *resultData = (int64_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    if (duckdb_validity_row_is_valid(indexVecValidity, row) &&
        duckdb_validity_row_is_valid(resVecValidity, row)) {
      H3Index index = IndexFromVector(indexVecData, row);
      auto res = resVecData[row];
      int64_t out;

      H3Error err = cellToChildPos(index, res, &out);
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
void ChildPosToCellFunction(duckdb_function_info info, duckdb_data_chunk input,
                            duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector posVec = duckdb_data_chunk_get_vector(input, 0);
  int64_t *posVecData = (int64_t *)duckdb_vector_get_data(posVec);
  uint64_t *posVecValidity = duckdb_vector_get_validity(posVec);
  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 1);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 2);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_vector_ensure_validity_writable(output);
  T *resultData = (T *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    if (duckdb_validity_row_is_valid(posVecValidity, row) &&
        duckdb_validity_row_is_valid(indexVecValidity, row) &&
        duckdb_validity_row_is_valid(resVecValidity, row)) {
      auto pos = posVecData[row];
      H3Index index = IndexFromVector(indexVecData, row);
      auto res = resVecData[row];

      H3Index out;
      H3Error err = childPosToCell(pos, index, res, &out);
      if (!err) {
        AssignHexString(output, resultData, row, out);
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void CompactCellsFunction(duckdb_function_info info, duckdb_data_chunk input,
                          duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  duckdb_list_entry *indexVecData =
      (duckdb_list_entry *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);
  duckdb_vector indexChildVec = duckdb_list_vector_get_child(indexVec);
  T *indexChildVecData = (T *)duckdb_vector_get_data(indexChildVec);
  uint64_t *indexChildValidity = duckdb_vector_get_validity(indexChildVec);

  idx_t outputReserveSize = 0;
  std::vector<std::pair<bool, std::vector<H3Index>>> completeResults;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasNullOriginally =
        !duckdb_validity_row_is_valid(indexVecValidity, row);
    bool hasNullInput = wasNullOriginally;

    std::vector<H3Index> inputSet(wasNullOriginally ? 0
                                                    : indexVecData[row].length);
    for (idx_t j = 0; j < wasNullOriginally ? 0 : indexVecData[row].length;
         j++) {
      auto childRow = indexVecData[row].offset + j;
      if (duckdb_validity_row_is_valid(indexChildValidity, childRow)) {
        auto index = IndexFromVector(indexChildVecData, childRow);

        if (index) {
          inputSet[j] = index;
        } else {
          hasNullInput = true;
          break;
        }
      } else {
        hasNullInput = true;
        break;
      }
    }

    std::vector<H3Index> compacted(inputSet.size());
    H3Error err =
        hasNullInput ||
        compactCells(inputSet.data(), compacted.data(), inputSet.size());

    std::vector<H3Index> result;
    if (!err) {
      int64_t actual = 0;
      for (size_t k = 0; k < inputSet.size(); k++) {
        auto childVal = compacted[k];
        if (childVal) {
          result.push_back(childVal);
          actual++;
        }
      }

      outputReserveSize += actual;
      completeResults.push_back(std::make_pair(true, result));
    } else {
      completeResults.push_back(std::make_pair(false, result));
    }
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
    auto [wasValid, out] = completeResults[row];

    if (wasValid) {
      for (idx_t j = 0; j < out.size(); ++j) {
        AssignHexString(outputChildVec, resultData, resultOffset + j, out[j]);
      }

      entries[row].offset = resultOffset;
      entries[row].length = out.size();
      resultOffset += out.size();
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, outputReserveSize);
}

template <typename T>
void UncompactCellsFunction(duckdb_function_info info, duckdb_data_chunk input,
                            duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  duckdb_list_entry *indexVecData =
      (duckdb_list_entry *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);
  duckdb_vector indexChildVec = duckdb_list_vector_get_child(indexVec);
  T *indexChildVecData = (T *)duckdb_vector_get_data(indexChildVec);
  uint64_t *indexChildValidity = duckdb_vector_get_validity(indexChildVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *resData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  idx_t outputReserveSize = 0;
  std::vector<std::pair<bool, std::vector<H3Index>>> completeResults;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasNullOriginally =
        !duckdb_validity_row_is_valid(indexVecValidity, row) ||
        !duckdb_validity_row_is_valid(resVecValidity, row);
    bool hasNullInput = false;

    std::vector<H3Index> inputSet(wasNullOriginally ? 0
                                                    : indexVecData[row].length);
    for (idx_t j = 0; j < wasNullOriginally ? 0 : indexVecData[row].length;
         j++) {
      auto childRow = indexVecData[row].offset + j;
      if (duckdb_validity_row_is_valid(indexChildValidity, childRow)) {
        auto index = IndexFromVector(indexChildVecData, childRow);

        if (index) {
          inputSet[j] = index;
        } else {
          hasNullInput = true;
          break;
        }
      } else {
        hasNullInput = true;
        break;
      }
    }

    auto res = resData[row];

    int64_t uncompactSize;
    std::vector<H3Index> result;

    H3Error err =
        hasNullInput || uncompactCellsSize(inputSet.data(), inputSet.size(),
                                           res, &uncompactSize);
    if (!err) {
      std::vector<H3Index> uncompacted(uncompactSize);
      H3Error err2 =
          uncompactCells(inputSet.data(), inputSet.size(), uncompacted.data(),
                         uncompacted.size(), res);

      if (!err2) {
        int64_t actual = 0;
        for (size_t k = 0; k < uncompacted.size(); k++) {
          auto childVal = uncompacted[k];
          if (childVal) {
            result.push_back(childVal);
            actual++;
          }
        }

        outputReserveSize += actual;
        completeResults.push_back(std::make_pair(true, result));
      } else {
        completeResults.push_back(std::make_pair(false, result));
      }
    } else {
      completeResults.push_back(std::make_pair(false, result));
    }
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
    auto [wasValid, out] = completeResults[row];

    if (wasValid) {
      for (idx_t j = 0; j < out.size(); ++j) {
        AssignHexString(outputChildVec, resultData, resultOffset + j, out[j]);
      }

      entries[row].offset = resultOffset;
      entries[row].length = out.size();
      resultOffset += out.size();
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, outputReserveSize);
}

duckdb_scalar_function_set H3Functions::GetCellToParentFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cell_to_parent");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_parent");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalType);
    duckdb_scalar_function_set_function(function,
                                        CellToParentFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCellToChildrenFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cell_to_children");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);
    duckdb_logical_type resultType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_children");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, resultType);
    duckdb_scalar_function_set_function(function,
                                        CellToChildrenFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&resultType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCellToChildrenSizeFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cell_to_children_size");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);

  auto r = [&functionSet, &intType,
            &bigintType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_children_size");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, bigintType);
    duckdb_scalar_function_set_function(
        function, CellToChildrenSizeFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&bigintType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCellToCenterChildFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cell_to_center_child");
  // Note this does not return an index, rather it returns a position ID

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_center_child");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalType);
    duckdb_scalar_function_set_function(
        function, CellToCenterChildFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCellToChildPosFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_cell_to_child_pos");
  // Note this does not return an index, rather it returns a position ID

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);

  auto r = [&functionSet, &intType,
            &bigintType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_cell_to_child_pos");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, bigintType);
    duckdb_scalar_function_set_function(function,
                                        CellToChildPosFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&bigintType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetChildPosToCellFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_child_pos_to_cell");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);

  auto r = [&functionSet, &intType,
            &bigintType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_child_pos_to_cell");
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalType);
    duckdb_scalar_function_set_function(function,
                                        ChildPosToCellFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&bigintType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetCompactCellsFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_compact_cells");

  auto r = [&functionSet]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);
    duckdb_logical_type listType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_compact_cells");
    duckdb_scalar_function_add_parameter(function, listType);
    duckdb_scalar_function_set_return_type(function, listType);
    duckdb_scalar_function_set_function(function,
                                        CompactCellsFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&listType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetUncompactCellsFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_uncompact_cells");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);
    duckdb_logical_type listType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_uncompact_cells");
    duckdb_scalar_function_add_parameter(function, listType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, listType);
    duckdb_scalar_function_set_function(function,
                                        UncompactCellsFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
    duckdb_destroy_logical_type(&listType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

} // namespace h3duckdb
