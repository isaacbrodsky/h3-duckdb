#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace h3duckdb {

struct GetResolutionOperator {
  static int32_t operate(H3Index index) { return getResolution(index); }
};

struct GetBaseCellNumberOperator {
  static int32_t operate(H3Index index) { return getBaseCellNumber(index); }
};

template <typename T>
void GetIndexDigitFunction(duckdb_function_info info, duckdb_data_chunk input,
                           duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector digitVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *digitVecData = (int32_t *)duckdb_vector_get_data(digitVec);

  duckdb_vector_ensure_validity_writable(output);
  uint64_t *resultData = (uint64_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    auto index = IndexFromVector(indexVecData, row);
    auto digit = digitVecData[row];

    int out;
    H3Error err = getIndexDigit(index, digit, &out);
    if (!err) {
      resultData[row] = out;
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

void StringToH3Function(duckdb_function_info info, duckdb_data_chunk input,
                        duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  duckdb_string_t *indexVecData =
      (duckdb_string_t *)duckdb_vector_get_data(indexVec);

  duckdb_vector_ensure_validity_writable(output);
  uint64_t *resultData = (uint64_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    auto index = &indexVecData[row];
    H3Index cell;

    auto cellStr = DuckdbToString(index);
    H3Error err = stringToH3(cellStr.c_str(), &cell);
    if (!err) {
      resultData[row] = cell;
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

template <typename T>
void H3ToStringFunction(duckdb_function_info info, duckdb_data_chunk input,
                        duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);

  duckdb_vector_ensure_validity_writable(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    auto str = ToHexString(indexVecData[row]);
    duckdb_vector_assign_string_element_len(output, row, str.c_str(),
                                            str.size());
  }
}

struct IsValidIndexOperator {
  static bool operate(H3Index index) { return isValidIndex(index); }
};

struct IsValidCellOperator {
  static bool operate(H3Index index) { return isValidCell(index); }
};

struct IsPentagonOperator {
  static bool operate(H3Index index) { return isPentagon(index); }
};

struct IsResClassIIIOperator {
  static bool operate(H3Index index) { return isResClassIII(index); }
};

template <typename T>
void GetIcosahedronFacesFunction(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);

  // Worst case scenario: all pentagons
  duckdb_list_vector_reserve(output, inputSize * 5);
  duckdb_vector_ensure_validity_writable(output);
  duckdb_list_entry *entries =
      (duckdb_list_entry *)duckdb_vector_get_data(output);
  duckdb_vector outputChildVec = duckdb_list_vector_get_child(output);
  int32_t *resultData = (int32_t *)duckdb_vector_get_data(outputChildVec);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);
  idx_t resultOffset = 0;

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;
    H3Index cell = IndexFromVector(indexVecData, row);

    if (cell) {
      int faceCount;

      H3Error err = maxFaceCount(cell, &faceCount);
      if (!err) {
        std::vector<int> out(faceCount);
        H3Error err2 = getIcosahedronFaces(cell, out.data());
        if (!err2) {
          idx_t actualCount = 0;
          for (idx_t face = 0; face < faceCount; ++face) {
            if (out[face] != -1) {
              resultData[resultOffset + actualCount] = out[face];
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
      entries[row].offset = resultOffset;
      entries[row].length = 0;
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

template <typename T>
void ConstructCellFunction(duckdb_function_info info, duckdb_data_chunk input,
                           duckdb_vector output) {
  auto hasRes = duckdb_data_chunk_get_column_count(input) >= 3;
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector baseCellVec = duckdb_data_chunk_get_vector(input, 0);
  int32_t *baseCellVecData = (int32_t *)duckdb_vector_get_data(baseCellVec);
  duckdb_vector digitsVec = duckdb_data_chunk_get_vector(input, 1);
  duckdb_list_entry *digitsVecData =
      (duckdb_list_entry *)duckdb_vector_get_data(digitsVec);
  duckdb_vector digitsChildVec = duckdb_list_vector_get_child(digitsVec);
  int32_t *digitsChildData = (int32_t *)duckdb_vector_get_data(digitsChildVec);
  uint64_t *digitsChildValidity = duckdb_vector_get_validity(digitsChildVec);

  int32_t *resData = nullptr;
  if (hasRes) {
    duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 2);
    resData = (int32_t *)duckdb_vector_get_data(resVec);
  }

  duckdb_vector_ensure_validity_writable(output);
  T *resultData = (T *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    auto baseCell = baseCellVecData[row];
    auto digitsEntry = digitsVecData[row];

    std::vector<int> digits(digitsEntry.length);
    bool digitsContainsNull = false;
    for (idx_t j = 0; j < digitsEntry.length; j++) {
      if (!duckdb_validity_row_is_valid(digitsChildValidity,
                                        digitsEntry.offset + j)) {
        digitsContainsNull = true;
        break;
      }
      digits[j] = digitsChildData[digitsEntry.offset + j];
    }

    auto res = hasRes ? resData[row] : static_cast<int32_t>(digits.size());

    if (digits.size() == res && !digitsContainsNull) {
      H3Index out;
      H3Error err = constructCell(res, baseCell, digits.data(), &out);
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

duckdb_scalar_function_set H3Functions::GetGetIndexDigitFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_get_index_digit");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_get_index_digit");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, intType);
    duckdb_scalar_function_set_function(function,
                                        GetIndexDigitFunction<PhysicalType>);
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

duckdb_scalar_function_set H3Functions::GetGetResolutionFunction() {
  return GetGenericInspectFunction<int32_t, GetResolutionOperator>(
      "h3_get_resolution", DUCKDB_TYPE_INTEGER);
}

duckdb_scalar_function_set H3Functions::GetGetBaseCellNumberFunction() {
  return GetGenericInspectFunction<int32_t, GetBaseCellNumberOperator>(
      "h3_get_base_cell_number", DUCKDB_TYPE_INTEGER);
}

duckdb_scalar_function H3Functions::GetStringToH3Function() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_string_to_h3");
  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_scalar_function_add_parameter(function, varcharType);
  duckdb_scalar_function_set_return_type(function, ubigintType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&ubigintType);
  duckdb_scalar_function_set_function(function, StringToH3Function);
  return function;
}

duckdb_scalar_function_set H3Functions::GetH3ToStringFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_h3_to_string");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

  auto r = [&functionSet,
            &varcharType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_h3_to_string");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, varcharType);
    duckdb_scalar_function_set_function(function, H3ToStringFunction<int64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);

  duckdb_destroy_logical_type(&varcharType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetIsValidIndexFunction() {
  return GetGenericInspectFunction<bool, IsValidIndexOperator>(
      "h3_is_valid_index", DUCKDB_TYPE_BOOLEAN);
}

duckdb_scalar_function_set H3Functions::GetIsValidCellFunction() {
  return GetGenericInspectFunction<bool, IsValidCellOperator>(
      "h3_is_valid_cell", DUCKDB_TYPE_BOOLEAN);
}

duckdb_scalar_function_set H3Functions::GetIsResClassIIIFunction() {
  return GetGenericInspectFunction<bool, IsResClassIIIOperator>(
      "h3_is_res_class_iii", DUCKDB_TYPE_BOOLEAN);
}

duckdb_scalar_function_set H3Functions::GetIsPentagonFunction() {
  return GetGenericInspectFunction<bool, IsPentagonOperator>(
      "h3_is_pentagon", DUCKDB_TYPE_BOOLEAN);
}

duckdb_scalar_function_set H3Functions::GetGetIcosahedronFacesFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_get_icosahedron_faces");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type intListType = duckdb_create_list_type(intType);

  auto r = [&functionSet,
            &intListType]<typename PhysicalType>(duckdb_type typeId) {
    duckdb_logical_type logicalType = duckdb_create_logical_type(typeId);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_get_icosahedron_faces");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_set_return_type(function, intListType);
    duckdb_scalar_function_set_function(
        function, GetIcosahedronFacesFunction<PhysicalType>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intListType);
  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetConstructCellFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_construct_cell");

  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type intListType = duckdb_create_list_type(intType);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_construct_cell");
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_add_parameter(function, intListType);
    duckdb_scalar_function_set_return_type(function, ubigintType);
    duckdb_scalar_function_set_function(function,
                                        ConstructCellFunction<uint64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_construct_cell");
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_add_parameter(function, intListType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, ubigintType);
    duckdb_scalar_function_set_function(function,
                                        ConstructCellFunction<uint64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&intListType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&ubigintType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetConstructCellVarcharFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_construct_cell_string");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type intListType = duckdb_create_list_type(intType);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_construct_cell_string");
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_add_parameter(function, intListType);
    duckdb_scalar_function_set_return_type(function, varcharType);
    duckdb_scalar_function_set_function(function,
                                        ConstructCellFunction<duckdb_string_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_construct_cell_string");
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_add_parameter(function, intListType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, varcharType);
    duckdb_scalar_function_set_function(function,
                                        ConstructCellFunction<duckdb_string_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&intListType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&varcharType);

  return functionSet;
}
} // namespace h3duckdb
