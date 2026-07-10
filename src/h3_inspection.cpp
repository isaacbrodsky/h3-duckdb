#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace h3duckdb {

// template <typename T>
// static void GetResolutionFunction(DataChunk &args, ExpressionState &state,
//                                  Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::Execute<T, int>(inputs, result, args.size(),
//                                 [&](T cell) { return getResolution(cell); });
//}
//
// static void GetResolutionVarcharFunction(DataChunk &args,
//                                         ExpressionState &state,
//                                         Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::ExecuteWithNulls<string_t, int>(
//      inputs, result, args.size(),
//      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
//        H3Index cell;
//        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
//        if (err0) {
//          mask.SetInvalid(idx);
//          return 0;
//        } else {
//          return getResolution(cell);
//        }
//      });
//}
//
// template <typename T>
// static void GetBaseCellNumberFunction(DataChunk &args, ExpressionState
// &state,
//                                      Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::Execute<T, int>(inputs, result, args.size(), [&](T cell) {
//    return getBaseCellNumber(cell);
//  });
//}
//
// static void GetBaseCellNumberVarcharFunction(DataChunk &args,
//                                             ExpressionState &state,
//                                             Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::ExecuteWithNulls<string_t, int>(
//      inputs, result, args.size(),
//      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
//        H3Index cell;
//        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
//        if (err0) {
//          mask.SetInvalid(idx);
//          return 0;
//        } else {
//          return getBaseCellNumber(cell);
//        }
//      });
//}
//
// template <typename T>
// static void GetIndexDigitFunction(DataChunk &args, ExpressionState &state,
//                                  Vector &result) {
//  auto &inputs = args.data[0];
//  auto &inputs2 = args.data[1];
//  BinaryExecutor::ExecuteWithNulls<T, int, int>(
//      inputs, inputs2, result, args.size(),
//      [&](T cell, int res, ValidityMask &mask, idx_t idx) {
//        int out;
//        H3Error err0 = getIndexDigit(cell, res, &out);
//        if (err0) {
//          mask.SetInvalid(idx);
//          return 0;
//        } else {
//          return out;
//        }
//      });
//}
//
// static void GetIndexDigitVarcharFunction(DataChunk &args,
//                                         ExpressionState &state,
//                                         Vector &result) {
//  auto &inputs = args.data[0];
//  auto &inputs2 = args.data[1];
//  BinaryExecutor::ExecuteWithNulls<string_t, int, int>(
//      inputs, inputs2, result, args.size(),
//      [&](string_t cellAddress, int res, ValidityMask &mask, idx_t idx) {
//        H3Index cell;
//        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
//        if (err0) {
//          mask.SetInvalid(idx);
//          return 0;
//        } else {
//          int out;
//          H3Error err1 = getIndexDigit(cell, res, &out);
//          if (err1) {
//            mask.SetInvalid(idx);
//            return 0;
//          } else {
//            return out;
//          }
//        }
//      });
//}

void StringToH3Function(duckdb_function_info info, duckdb_data_chunk input,
                        duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  duckdb_string_t *indexVecData =
      (duckdb_string_t *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);

  duckdb_vector_ensure_validity_writable(output);
  uint64_t *resultData = (uint64_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    if (duckdb_validity_row_is_valid(indexVecValidity, row)) {
      auto index = &indexVecData[row];
      H3Index cell;

      H3Error err = stringToH3(duckdb_string_t_data(index), &cell);
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

template <typename T>
void H3ToStringFunction(duckdb_function_info info, duckdb_data_chunk input,
                        duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  uint64_t *indexVecValidity = duckdb_vector_get_validity(indexVec);

  duckdb_vector_ensure_validity_writable(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    if (duckdb_validity_row_is_valid(indexVecValidity, row)) {
      auto str = ToHexString(indexVecData[row]);
      duckdb_vector_assign_string_element_len(output, row, str.c_str(),
                                              str.size());
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

struct IsValidIndexOperator {
  static bool operate(H3Index index) { return isValidIndex(index); }
};

struct IsValidCellOperator {
  static bool operate(H3Index index) { return isValidCell(index); }
};

template <typename T, typename Operator>
void IsValidGenericFunction(duckdb_function_info info, duckdb_data_chunk input,
                            duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);

  bool *resultData = (bool *)duckdb_vector_get_data(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    auto cell = IndexFromVector(indexVecData, row);

    H3Error err = Operator::operate(cell);
    resultData[row] = !err;
  }
}

// template <typename T>
// static void IsResClassIIIFunction(DataChunk &args, ExpressionState &state,
//                                  Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::Execute<T, bool>(inputs, result, args.size(), [&](T cell) {
//    return bool(isResClassIII(cell));
//  });
//}
//
// static void IsResClassIIIVarcharFunction(DataChunk &args,
//                                         ExpressionState &state,
//                                         Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::ExecuteWithNulls<string_t, bool>(
//      inputs, result, args.size(),
//      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
//        H3Index cell;
//        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
//        if (err0) {
//          mask.SetInvalid(idx);
//          return false;
//        } else {
//          return bool(isResClassIII(cell));
//        }
//      });
//}
//
// template <typename T>
// static void IsPentagonFunction(DataChunk &args, ExpressionState &state,
//                               Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::Execute<T, bool>(inputs, result, args.size(), [&](T cell) {
//    return bool(isPentagon(cell));
//  });
//}
//
// static void IsPentagonVarcharFunction(DataChunk &args, ExpressionState
// &state,
//                                      Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::ExecuteWithNulls<string_t, bool>(
//      inputs, result, args.size(),
//      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
//        H3Index cell;
//        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
//        if (err0) {
//          mask.SetInvalid(idx);
//          return false;
//        } else {
//          return bool(isPentagon(cell));
//        }
//      });
//}
//
// static void GetIcosahedronFacesFunction(DataChunk &args, ExpressionState
// &state,
//                                        Vector &result) {
//  auto result_data = FlatVector::GetData<list_entry_t>(result);
//  for (idx_t i = 0; i < args.size(); i++) {
//    result_data[i].offset = ListVector::GetListSize(result);
//
//    uint64_t cell = args.GetValue(0, i)
//                        .DefaultCastAs(LogicalType::UBIGINT)
//                        .GetValue<uint64_t>();
//    int faceCount;
//    int64_t actual = 0;
//    H3Error err1 = maxFaceCount(cell, &faceCount);
//    if (err1) {
//      result.SetValue(i, Value(LogicalType::SQLNULL));
//    } else {
//      std::vector<int> out(faceCount);
//      H3Error err2 = getIcosahedronFaces(cell, out.data());
//      if (err2) {
//        result.SetValue(i, Value(LogicalType::SQLNULL));
//      } else {
//        for (auto val : out) {
//          if (val != -1) {
//            ListVector::PushBack(result, Value::INTEGER(val));
//            actual++;
//          }
//        }
//      }
//    }
//
//    result_data[i].length = actual;
//  }
//  if (args.AllConstant()) {
//    result.SetVectorType(VectorType::CONSTANT_VECTOR);
//  }
//  result.Verify(args.size());
//}
//
// static void GetIcosahedronFacesVarcharFunction(DataChunk &args,
//                                               ExpressionState &state,
//                                               Vector &result) {
//  result.SetVectorType(VectorType::FLAT_VECTOR);
//  auto result_data = FlatVector::GetData<list_entry_t>(result);
//  for (idx_t i = 0; i < args.size(); i++) {
//    result_data[i].offset = ListVector::GetListSize(result);
//
//    int faceCount;
//    int64_t actual = 0;
//    string cellAddress = args.GetValue(0, i)
//                             .DefaultCastAs(LogicalType::VARCHAR)
//                             .GetValue<string>();
//    H3Index cell;
//    H3Error err0 = stringToH3(cellAddress.c_str(), &cell);
//    if (err0) {
//      result.SetValue(i, Value(LogicalType::SQLNULL));
//    } else {
//      H3Error err1 = maxFaceCount(cell, &faceCount);
//      if (err1) {
//        result.SetValue(i, Value(LogicalType::SQLNULL));
//      } else {
//        std::vector<int> out(faceCount);
//        H3Error err2 = getIcosahedronFaces(cell, out.data());
//        if (err2) {
//          result.SetValue(i, Value(LogicalType::SQLNULL));
//        } else {
//          for (auto val : out) {
//            if (val != -1) {
//              ListVector::PushBack(result, Value::INTEGER(val));
//              actual++;
//            }
//          }
//        }
//      }
//    }
//
//    result_data[i].length = actual;
//  }
//  if (args.AllConstant()) {
//    result.SetVectorType(VectorType::CONSTANT_VECTOR);
//  }
//  result.Verify(args.size());
//}
//
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
//
// CreateScalarFunctionInfo H3Functions::GetGetResolutionFunction() {
//  ScalarFunctionSet funcs("h3_get_resolution");
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
//  LogicalType::INTEGER,
//                                   GetResolutionFunction<uint64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
//  LogicalType::INTEGER,
//                                   GetResolutionFunction<int64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
//  LogicalType::INTEGER,
//                                   GetResolutionVarcharFunction));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetGetBaseCellNumberFunction() {
//  ScalarFunctionSet funcs("h3_get_base_cell_number");
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
//  LogicalType::INTEGER,
//                                   GetBaseCellNumberFunction<uint64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
//  LogicalType::INTEGER,
//                                   GetBaseCellNumberFunction<int64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
//  LogicalType::INTEGER,
//                                   GetBaseCellNumberVarcharFunction));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetGetIndexDigitFunction() {
//  ScalarFunctionSet funcs("h3_get_index_digit");
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT,
//  LogicalType::INTEGER},
//                                   LogicalType::INTEGER,
//                                   GetIndexDigitFunction<uint64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT,
//  LogicalType::INTEGER},
//                                   LogicalType::INTEGER,
//                                   GetIndexDigitFunction<int64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR,
//  LogicalType::INTEGER},
//                                   LogicalType::INTEGER,
//                                   GetIndexDigitVarcharFunction));
//  return CreateScalarFunctionInfo(funcs);
//}

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
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_is_valid_index");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type boolType =
      duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_is_valid_index");
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_set_return_type(function, boolType);
    duckdb_scalar_function_set_function(
        function, IsValidGenericFunction<uint64_t, IsValidIndexOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_is_valid_index");
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_set_return_type(function, varcharType);
    duckdb_scalar_function_set_function(
        function, IsValidGenericFunction<int64_t, IsValidIndexOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_is_valid_index");
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_set_return_type(function, varcharType);
    duckdb_scalar_function_set_function(
        function,
        IsValidGenericFunction<duckdb_string_t, IsValidIndexOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&boolType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);

  return functionSet;
}

duckdb_scalar_function_set H3Functions::GetIsValidCellFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_is_valid_cell");

  duckdb_logical_type varcharType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_logical_type boolType =
      duckdb_create_logical_type(DUCKDB_TYPE_BOOLEAN);

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_is_valid_cell");
    duckdb_scalar_function_add_parameter(function, ubigintType);
    duckdb_scalar_function_set_return_type(function, boolType);
    duckdb_scalar_function_set_function(
        function, IsValidGenericFunction<uint64_t, IsValidCellOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_is_valid_cell");
    duckdb_scalar_function_add_parameter(function, bigintType);
    duckdb_scalar_function_set_return_type(function, varcharType);
    duckdb_scalar_function_set_function(
        function, IsValidGenericFunction<int64_t, IsValidCellOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  {
    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_is_valid_cell");
    duckdb_scalar_function_add_parameter(function, varcharType);
    duckdb_scalar_function_set_return_type(function, varcharType);
    duckdb_scalar_function_set_function(
        function, IsValidGenericFunction<duckdb_string_t, IsValidCellOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);
  }

  duckdb_destroy_logical_type(&boolType);
  duckdb_destroy_logical_type(&varcharType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_destroy_logical_type(&ubigintType);

  return functionSet;
}

// CreateScalarFunctionInfo H3Functions::GetIsResClassIIIFunction() {
//  ScalarFunctionSet funcs("h3_is_res_class_iii");
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
//  LogicalType::BOOLEAN,
//                                   IsResClassIIIFunction<uint64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
//  LogicalType::BOOLEAN,
//                                   IsResClassIIIFunction<int64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
//  LogicalType::BOOLEAN,
//                                   IsResClassIIIVarcharFunction));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetIsPentagonFunction() {
//  ScalarFunctionSet funcs("h3_is_pentagon");
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
//  LogicalType::BOOLEAN,
//                                   IsPentagonFunction<uint64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
//  LogicalType::BOOLEAN,
//                                   IsPentagonFunction<int64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
//  LogicalType::BOOLEAN,
//                                   IsPentagonVarcharFunction));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetGetIcosahedronFacesFunction() {
//  ScalarFunctionSet funcs("h3_get_icosahedron_faces");
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
//                                   LogicalType::LIST(LogicalType::INTEGER),
//                                   GetIcosahedronFacesFunction));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
//                                   LogicalType::LIST(LogicalType::INTEGER),
//                                   GetIcosahedronFacesFunction));
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
//                                   LogicalType::LIST(LogicalType::INTEGER),
//                                   GetIcosahedronFacesVarcharFunction));
//  return CreateScalarFunctionInfo(funcs);
//}
//
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
