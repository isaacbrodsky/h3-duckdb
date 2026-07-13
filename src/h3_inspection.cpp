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
    H3Index cell;

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

    H3Error err = stringToH3(duckdb_string_t_data(index), &cell);
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

template <typename T, typename U, typename Operator>
void InspectGenericFunction(duckdb_function_info info, duckdb_data_chunk input,
                            duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);

  U *resultData = (U *)duckdb_vector_get_data(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    auto cell = IndexFromVector(indexVecData, row);

    resultData[row] = Operator::operate(cell);
  }
}

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
}

// static void ConstructCellFunction(DataChunk &args, ExpressionState &state,
//                                  Vector &result) {
//  D_ASSERT(args.ColumnCount() == 3 || args.ColumnCount() == 2);
//  auto count = args.size();
//  bool hasRes = args.ColumnCount() == 3;
//  bool resVecConstant = true;
//
//  Vector &baseCellVec = args.data[0];
//  Vector &digitsVec = args.data[1];
//  UnifiedVectorFormat res_data;
//  if (hasRes) {
//    Vector &resVec = args.data[2];
//    if (resVec.GetType().id() == LogicalTypeId::SQLNULL) {
//      result.Reference(resVec);
//      return;
//    }
//    resVecConstant = resVec.GetVectorType() == VectorType::CONSTANT_VECTOR;
//    resVec.ToUnifiedFormat(count, res_data);
//  }
//  if (baseCellVec.GetType().id() == LogicalTypeId::SQLNULL) {
//    result.Reference(baseCellVec);
//    return;
//  }
//  if (digitsVec.GetType().id() == LogicalTypeId::SQLNULL) {
//    result.Reference(digitsVec);
//    return;
//  }
//
//  auto lists_size = ListVector::GetListSize(digitsVec);
//  auto &child_vector = ListVector::GetEntry(digitsVec);
//  child_vector.Flatten(lists_size);
//
//  UnifiedVectorFormat child_data;
//  child_vector.ToUnifiedFormat(lists_size, child_data);
//
//  UnifiedVectorFormat lists_data;
//  digitsVec.ToUnifiedFormat(count, lists_data);
//  auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);
//
//  UnifiedVectorFormat base_cell_data;
//  baseCellVec.ToUnifiedFormat(count, base_cell_data);
//
//  result.SetVectorType(VectorType::FLAT_VECTOR);
//  auto result_entries = FlatVector::GetData<uint64_t>(result);
//  auto &result_validity = FlatVector::Validity(result);
//
//  idx_t offset = 0;
//  for (idx_t i = 0; i < count; i++) {
//    auto list_index = lists_data.sel->get_index(i);
//    if (!lists_data.validity.RowIsValid(list_index) ||
//        (hasRes && !res_data.validity.RowIsValid(i)) ||
//        !base_cell_data.validity.RowIsValid(i)) {
//      result_validity.SetInvalid(i);
//      continue;
//    }
//
//    auto baseCell = baseCellVec.GetValue(i)
//                        .DefaultCastAs(LogicalType::INTEGER)
//                        .GetValue<int>();
//
//    vector<int> digits(list_entries[i].length);
//    for (size_t j = 0; j < list_entries[i].length; j++) {
//      if (child_data.validity.RowIsValid(
//              child_data.sel->get_index(list_entries[i].offset + j))) {
//        digits[j] =
//            ((int *)child_data
//                 .data)[child_data.sel->get_index(list_entries[i].offset +
//                 j)];
//      }
//    }
//
//    auto res = hasRes ? args.data[2]
//                            .GetValue(i)
//                            .DefaultCastAs(LogicalType::INTEGER)
//                            .GetValue<int>()
//                      : digits.size();
//
//    if (list_entries[i].length != res) {
//      result_validity.SetInvalid(i);
//      continue;
//    }
//
//    H3Index out;
//    H3Error err = constructCell(res, baseCell, digits.data(), &out);
//    if (err) {
//      result_validity.SetInvalid(i);
//    } else {
//      result.SetValue(i, Value::UBIGINT(out));
//    }
//  }
//
//  if (resVecConstant &&
//      baseCellVec.GetVectorType() == VectorType::CONSTANT_VECTOR &&
//      digitsVec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
//    result.SetVectorType(VectorType::CONSTANT_VECTOR);
//  }
//  result.Verify(args.size());
//}
//
// static void ConstructCellVarcharFunction(DataChunk &args,
//                                         ExpressionState &state,
//                                         Vector &result) {
//  D_ASSERT(args.ColumnCount() == 3 || args.ColumnCount() == 2);
//  auto count = args.size();
//  bool hasRes = args.ColumnCount() == 3;
//  bool resVecConstant = true;
//
//  Vector &baseCellVec = args.data[0];
//  Vector &digitsVec = args.data[1];
//  UnifiedVectorFormat res_data;
//  if (hasRes) {
//    Vector &resVec = args.data[2];
//    if (resVec.GetType().id() == LogicalTypeId::SQLNULL) {
//      result.Reference(resVec);
//      return;
//    }
//    resVecConstant = resVec.GetVectorType() == VectorType::CONSTANT_VECTOR;
//    resVec.ToUnifiedFormat(count, res_data);
//  }
//  if (baseCellVec.GetType().id() == LogicalTypeId::SQLNULL) {
//    result.Reference(baseCellVec);
//    return;
//  }
//  if (digitsVec.GetType().id() == LogicalTypeId::SQLNULL) {
//    result.Reference(digitsVec);
//    return;
//  }
//
//  auto lists_size = ListVector::GetListSize(digitsVec);
//  auto &child_vector = ListVector::GetEntry(digitsVec);
//  child_vector.Flatten(lists_size);
//
//  UnifiedVectorFormat child_data;
//  child_vector.ToUnifiedFormat(lists_size, child_data);
//
//  UnifiedVectorFormat lists_data;
//  digitsVec.ToUnifiedFormat(count, lists_data);
//  auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);
//
//  UnifiedVectorFormat base_cell_data;
//  baseCellVec.ToUnifiedFormat(count, base_cell_data);
//
//  result.SetVectorType(VectorType::FLAT_VECTOR);
//  auto result_entries = FlatVector::GetData<string_t>(result);
//  auto &result_validity = FlatVector::Validity(result);
//
//  idx_t offset = 0;
//  for (idx_t i = 0; i < count; i++) {
//    auto list_index = lists_data.sel->get_index(i);
//    if (!lists_data.validity.RowIsValid(list_index) ||
//        (hasRes && !res_data.validity.RowIsValid(i)) ||
//        !base_cell_data.validity.RowIsValid(i)) {
//      result_validity.SetInvalid(i);
//      continue;
//    }
//
//    auto baseCell = baseCellVec.GetValue(i)
//                        .DefaultCastAs(LogicalType::INTEGER)
//                        .GetValue<int>();
//
//    vector<int> digits(list_entries[i].length);
//    for (size_t j = 0; j < list_entries[i].length; j++) {
//      if (child_data.validity.RowIsValid(
//              child_data.sel->get_index(list_entries[i].offset + j))) {
//        digits[j] =
//            ((int *)child_data
//                 .data)[child_data.sel->get_index(list_entries[i].offset +
//                 j)];
//      }
//    }
//
//    auto res = hasRes ? args.data[2]
//                            .GetValue(i)
//                            .DefaultCastAs(LogicalType::INTEGER)
//                            .GetValue<int>()
//                      : digits.size();
//
//    if (list_entries[i].length != res) {
//      result_validity.SetInvalid(i);
//      continue;
//    }
//
//    H3Index out;
//    H3Error err = constructCell(res, baseCell, digits.data(), &out);
//    if (err) {
//      result_validity.SetInvalid(i);
//    } else {
//      auto str = StringUtil::Format("%llx", out);
//      result.SetValue(i, StringVector::AddString(result, str));
//    }
//  }
//
//  if (resVecConstant &&
//      baseCellVec.GetVectorType() == VectorType::CONSTANT_VECTOR &&
//      digitsVec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
//    result.SetVectorType(VectorType::CONSTANT_VECTOR);
//  }
//  result.Verify(args.size());
//}

duckdb_scalar_function_set H3Functions::GetGetIndexDigitFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_get_index_digit");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_get_index_digit");
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, intType);
    duckdb_scalar_function_set_function(function,
                                        GetIndexDigitFunction<int64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_get_index_digit");
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, intType);
    duckdb_scalar_function_set_function(function,
                                        GetIndexDigitFunction<uint64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_get_index_digit");
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, intType);
    duckdb_scalar_function_set_function(function,
                                        GetIndexDigitFunction<duckdb_string_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);
  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

template <typename ResultType, typename Operator>
static duckdb_scalar_function_set
GetGenericInspectFunction(const char *name, duckdb_type returnTypeId) {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set(name);

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type returnType = duckdb_create_logical_type(returnTypeId);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, name);
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_set_return_type(function, returnType);
    duckdb_scalar_function_set_function(
        function, InspectGenericFunction<uint64_t, ResultType, Operator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, name);
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_set_return_type(function, returnType);
    duckdb_scalar_function_set_function(
        function, InspectGenericFunction<int64_t, ResultType, Operator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, name);
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_set_return_type(function, returnType);
    duckdb_scalar_function_set_function(
        function,
        InspectGenericFunction<duckdb_string_t, ResultType, Operator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&returnType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);

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
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_h3_to_string");
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_set_return_type(function, varcharType);
    duckdb_scalar_function_set_function(function, H3ToStringFunction<int64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_h3_to_string");
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_set_return_type(function, varcharType);
    duckdb_scalar_function_set_function(function, H3ToStringFunction<uint64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);

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

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type intListType = duckdb_create_list_type(intType);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_get_icosahedron_faces");
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_set_return_type(function, intListType);
    duckdb_scalar_function_set_function(function,
                                        GetIcosahedronFacesFunction<int64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_get_icosahedron_faces");
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_set_return_type(function, intListType);
    duckdb_scalar_function_set_function(function,
                                        GetIcosahedronFacesFunction<uint64_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_get_icosahedron_faces");
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_set_return_type(function, intListType);
    duckdb_scalar_function_set_function(
        function, GetIcosahedronFacesFunction<duckdb_string_t>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&intListType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);

  return functionSet;
}

// CreateScalarFunctionInfo H3Functions::GetConstructCellFunction() {
//  ScalarFunctionSet funcs("h3_construct_cell");
//  funcs.AddFunction(ScalarFunction(
//      "h3_construct_cell",
//      {LogicalType::INTEGER, LogicalType::LIST(LogicalType::INTEGER)},
//      LogicalType::UBIGINT, ConstructCellFunction));
//  funcs.AddFunction(ScalarFunction(
//      "h3_construct_cell",
//      {LogicalType::INTEGER, LogicalType::LIST(LogicalType::INTEGER),
//       LogicalType::INTEGER},
//      LogicalType::UBIGINT, ConstructCellFunction));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetConstructCellVarcharFunction() {
//  ScalarFunctionSet funcs("h3_construct_cell_string");
//  funcs.AddFunction(ScalarFunction(
//      "h3_construct_cell_string",
//      {LogicalType::INTEGER, LogicalType::LIST(LogicalType::INTEGER)},
//      LogicalType::VARCHAR, ConstructCellVarcharFunction));
//  funcs.AddFunction(ScalarFunction(
//      "h3_construct_cell_string",
//      {LogicalType::INTEGER, LogicalType::LIST(LogicalType::INTEGER),
//       LogicalType::INTEGER},
//      LogicalType::VARCHAR, ConstructCellVarcharFunction));
//  return CreateScalarFunctionInfo(funcs);
//}

} // namespace h3duckdb
