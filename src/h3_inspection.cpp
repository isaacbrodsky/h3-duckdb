#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

template <typename T>
static void GetResolutionFunction(DataChunk &args, ExpressionState &state,
                                  Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<T, int>(inputs, result, args.size(),
                                 [&](T cell) { return getResolution(cell); });
}

static void GetResolutionVarcharFunction(DataChunk &args,
                                         ExpressionState &state,
                                         Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<string_t, int>(
      inputs, result, args.size(),
      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
        H3Index cell;
        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
        if (err0) {
          mask.SetInvalid(idx);
          return 0;
        } else {
          return getResolution(cell);
        }
      });
}

template <typename T>
static void GetBaseCellNumberFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<T, int>(inputs, result, args.size(), [&](T cell) {
    return getBaseCellNumber(cell);
  });
}

static void GetBaseCellNumberVarcharFunction(DataChunk &args,
                                             ExpressionState &state,
                                             Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<string_t, int>(
      inputs, result, args.size(),
      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
        H3Index cell;
        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
        if (err0) {
          mask.SetInvalid(idx);
          return 0;
        } else {
          return getBaseCellNumber(cell);
        }
      });
}

template <typename T>
static void GetIndexDigitFunction(DataChunk &args, ExpressionState &state,
                                  Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<T, int, int>(
      inputs, inputs2, result, args.size(),
      [&](T cell, int res, ValidityMask &mask, idx_t idx) {
        int out;
        H3Error err0 = getIndexDigit(cell, res, &out);
        if (err0) {
          mask.SetInvalid(idx);
          return 0;
        } else {
          return out;
        }
      });
}

static void GetIndexDigitVarcharFunction(DataChunk &args,
                                         ExpressionState &state,
                                         Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<string_t, int, int>(
      inputs, inputs2, result, args.size(),
      [&](string_t cellAddress, int res, ValidityMask &mask, idx_t idx) {
        H3Index cell;
        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
        if (err0) {
          mask.SetInvalid(idx);
          return 0;
        } else {
          int out;
          H3Error err1 = getIndexDigit(cell, res, &out);
          if (err1) {
            mask.SetInvalid(idx);
            return 0;
          } else {
            return out;
          }
        }
      });
}

static void StringToH3Function(DataChunk &args, ExpressionState &state,
                               Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<string_t, uint64_t>(
      inputs, result, args.size(),
      [&](string_t input, ValidityMask &mask, idx_t idx) {
        H3Index h;
        H3Error err = stringToH3(input.GetString().c_str(), &h);
        if (err) {
          mask.SetInvalid(idx);
          return H3Index(H3_NULL);
        } else {
          return h;
        }
      });
}

struct H3ToStringOperator {
  template <class INPUT_TYPE, class RESULT_TYPE>
  static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
    auto str = StringUtil::Format("%llx", input);
    return StringVector::AddString(result, str);
  }
};

template <typename T>
static void H3ToStringFunction(DataChunk &args, ExpressionState &state,
                               Vector &result) {
  UnaryExecutor::ExecuteString<T, string_t, H3ToStringOperator>(
      args.data[0], result, args.size());
}

static void IsValidIndexVarcharFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<string_t, bool>(
      inputs, result, args.size(), [&](string_t input) {
        H3Index h;
        H3Error err = stringToH3(input.GetString().c_str(), &h);
        if (err) {
          return false;
        }
        return bool(isValidIndex(h));
      });
}

template <typename T>
static void IsValidIndexFunction(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<T, bool>(inputs, result, args.size(), [&](T input) {
    return bool(isValidIndex(input));
  });
}

static void IsValidCellVarcharFunction(DataChunk &args, ExpressionState &state,
                                       Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<string_t, bool>(
      inputs, result, args.size(), [&](string_t input) {
        H3Index h;
        H3Error err = stringToH3(input.GetString().c_str(), &h);
        if (err) {
          return false;
        }
        return bool(isValidCell(h));
      });
}

template <typename T>
static void IsValidCellFunction(DataChunk &args, ExpressionState &state,
                                Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<T, bool>(inputs, result, args.size(), [&](T input) {
    return bool(isValidCell(input));
  });
}

template <typename T>
static void IsResClassIIIFunction(DataChunk &args, ExpressionState &state,
                                  Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<T, bool>(inputs, result, args.size(), [&](T cell) {
    return bool(isResClassIII(cell));
  });
}

static void IsResClassIIIVarcharFunction(DataChunk &args,
                                         ExpressionState &state,
                                         Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<string_t, bool>(
      inputs, result, args.size(),
      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
        H3Index cell;
        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
        if (err0) {
          mask.SetInvalid(idx);
          return false;
        } else {
          return bool(isResClassIII(cell));
        }
      });
}

template <typename T>
static void IsPentagonFunction(DataChunk &args, ExpressionState &state,
                               Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<T, bool>(inputs, result, args.size(), [&](T cell) {
    return bool(isPentagon(cell));
  });
}

static void IsPentagonVarcharFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<string_t, bool>(
      inputs, result, args.size(),
      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
        H3Index cell;
        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
        if (err0) {
          mask.SetInvalid(idx);
          return false;
        } else {
          return bool(isPentagon(cell));
        }
      });
}

static void GetIcosahedronFacesFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    uint64_t cell = args.GetValue(0, i)
                        .DefaultCastAs(LogicalType::UBIGINT)
                        .GetValue<uint64_t>();
    int faceCount;
    int64_t actual = 0;
    H3Error err1 = maxFaceCount(cell, &faceCount);
    if (err1) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      std::vector<int> out(faceCount);
      H3Error err2 = getIcosahedronFaces(cell, out.data());
      if (err2) {
        result.SetValue(i, Value(LogicalType::SQLNULL));
      } else {
        for (auto val : out) {
          if (val != -1) {
            ListVector::PushBack(result, Value::INTEGER(val));
            actual++;
          }
        }
      }
    }

    result_data[i].length = actual;
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void GetIcosahedronFacesVarcharFunction(DataChunk &args,
                                               ExpressionState &state,
                                               Vector &result) {
  UnifiedVectorFormat vdata;
  args.data[0].ToUnifiedFormat(args.size(), vdata);

  auto ldata = UnifiedVectorFormat::GetData<string_t>(vdata);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    int faceCount;
    int64_t actual = 0;
    string_t cellAddress = ldata[i];
    H3Index cell;
    H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
    if (err0) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      H3Error err1 = maxFaceCount(cell, &faceCount);
      if (err1) {
        result.SetValue(i, Value(LogicalType::SQLNULL));
      } else {
        std::vector<int> out(faceCount);
        H3Error err2 = getIcosahedronFaces(cell, out.data());
        if (err2) {
          result.SetValue(i, Value(LogicalType::SQLNULL));
        } else {
          for (auto val : out) {
            if (val != -1) {
              ListVector::PushBack(result, Value::INTEGER(val));
              actual++;
            }
          }
        }
      }
    }

    result_data[i].length = actual;
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void ConstructCellFunction(DataChunk &args, ExpressionState &state,
                                  Vector &result) {
  D_ASSERT(args.ColumnCount() == 3);
  auto count = args.size();

  Vector &resVec = args.data[0];
  Vector &baseCellVec = args.data[1];
  Vector &digitsVec = args.data[2];
  if (resVec.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(resVec);
    return;
  }
  if (baseCellVec.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(baseCellVec);
    return;
  }
  if (digitsVec.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(digitsVec);
    return;
  }

  auto lists_size = ListVector::GetListSize(digitsVec);
  auto &child_vector = ListVector::GetEntry(digitsVec);
  child_vector.Flatten(lists_size);

  UnifiedVectorFormat child_data;
  child_vector.ToUnifiedFormat(lists_size, child_data);

  UnifiedVectorFormat lists_data;
  digitsVec.ToUnifiedFormat(count, lists_data);
  auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

  UnifiedVectorFormat res_data;
  resVec.ToUnifiedFormat(count, res_data);

  UnifiedVectorFormat base_cell_data;
  baseCellVec.ToUnifiedFormat(count, base_cell_data);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_entries = FlatVector::GetData<uint64_t>(result);
  auto &result_validity = FlatVector::Validity(result);

  idx_t offset = 0;
  for (idx_t i = 0; i < count; i++) {
    auto list_index = lists_data.sel->get_index(i);
    if (!lists_data.validity.RowIsValid(list_index) ||
        !res_data.validity.RowIsValid(i) ||
        !base_cell_data.validity.RowIsValid(i)) {
      result_validity.SetInvalid(i);
      continue;
    }

    auto res =
        resVec.GetValue(i).DefaultCastAs(LogicalType::INTEGER).GetValue<int>();
    auto baseCell =
        baseCellVec.GetValue(i).DefaultCastAs(LogicalType::INTEGER).GetValue<int>();

    if (list_entries[i].length != res) {
      result_validity.SetInvalid(i);
      continue;
    }

    vector<int> digits(list_entries[i].length);
    for (size_t j = 0; j < list_entries[i].length; j++) {
      if (child_data.validity.RowIsValid(
              child_data.sel->get_index(list_entries[i].offset + j))) {
        digits[j] = ((int *)child_data.data)[child_data.sel->get_index(list_entries[i].offset + j)];
      }
    }

    H3Index out;
    H3Error err = constructCell(res, baseCell, digits.data(), &out);
    if (err) {
      result_validity.SetInvalid(i);
    } else {
      result.SetValue(i, Value::UBIGINT(out));
    }
  }

  if (resVec.GetVectorType() == VectorType::CONSTANT_VECTOR &&
      baseCellVec.GetVectorType() == VectorType::CONSTANT_VECTOR &&
      digitsVec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void ConstructCellVarcharFunction(DataChunk &args,
                                         ExpressionState &state,
                                         Vector &result) {
  D_ASSERT(args.ColumnCount() == 3);
  auto count = args.size();

  Vector &resVec = args.data[0];
  Vector &baseCellVec = args.data[1];
  Vector &digitsVec = args.data[2];
  if (resVec.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(resVec);
    return;
  }
  if (baseCellVec.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(baseCellVec);
    return;
  }
  if (digitsVec.GetType().id() == LogicalTypeId::SQLNULL) {
    result.Reference(digitsVec);
    return;
  }

  auto lists_size = ListVector::GetListSize(digitsVec);
  auto &child_vector = ListVector::GetEntry(digitsVec);
  child_vector.Flatten(lists_size);

  UnifiedVectorFormat child_data;
  child_vector.ToUnifiedFormat(lists_size, child_data);

  UnifiedVectorFormat lists_data;
  digitsVec.ToUnifiedFormat(count, lists_data);
  auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(lists_data);

  UnifiedVectorFormat res_data;
  resVec.ToUnifiedFormat(count, res_data);

  UnifiedVectorFormat base_cell_data;
  baseCellVec.ToUnifiedFormat(count, base_cell_data);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_entries = FlatVector::GetData<string_t>(result);
  auto &result_validity = FlatVector::Validity(result);

  idx_t offset = 0;
  for (idx_t i = 0; i < count; i++) {
    auto list_index = lists_data.sel->get_index(i);
    if (!lists_data.validity.RowIsValid(list_index) ||
        !res_data.validity.RowIsValid(i) ||
        !base_cell_data.validity.RowIsValid(i)) {
      result_validity.SetInvalid(i);
      continue;
    }

    auto res =
        resVec.GetValue(i).DefaultCastAs(LogicalType::INTEGER).GetValue<int>();
    auto baseCell =
        baseCellVec.GetValue(i).DefaultCastAs(LogicalType::INTEGER).GetValue<int>();

    if (list_entries[i].length != res) {
      result_validity.SetInvalid(i);
      continue;
    }

    vector<int> digits(list_entries[i].length);
    for (size_t j = 0; j < list_entries[i].length; j++) {
      if (child_data.validity.RowIsValid(
              child_data.sel->get_index(list_entries[i].offset + j))) {
        digits[j] = ((int *)child_data.data)[child_data.sel->get_index(list_entries[i].offset + j)];
      }
    }

    H3Index out;
    H3Error err = constructCell(res, baseCell, digits.data(), &out);
    if (err) {
      result_validity.SetInvalid(i);
    } else {
      auto str = StringUtil::Format("%llx", out);
      result.SetValue(i, StringVector::AddString(result, str));
    }
  }

  if (resVec.GetVectorType() == VectorType::CONSTANT_VECTOR &&
      baseCellVec.GetVectorType() == VectorType::CONSTANT_VECTOR &&
      digitsVec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

CreateScalarFunctionInfo H3Functions::GetGetResolutionFunction() {
  ScalarFunctionSet funcs("h3_get_resolution");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::INTEGER,
                                   GetResolutionFunction<uint64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::INTEGER,
                                   GetResolutionFunction<int64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::INTEGER,
                                   GetResolutionVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetBaseCellNumberFunction() {
  ScalarFunctionSet funcs("h3_get_base_cell_number");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::INTEGER,
                                   GetBaseCellNumberFunction<uint64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::INTEGER,
                                   GetBaseCellNumberFunction<int64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::INTEGER,
                                   GetBaseCellNumberVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetIndexDigitFunction() {
  ScalarFunctionSet funcs("h3_get_index_digit");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::INTEGER},
                                   LogicalType::INTEGER,
                                   GetIndexDigitFunction<uint64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER},
                                   LogicalType::INTEGER,
                                   GetIndexDigitFunction<int64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER},
                                   LogicalType::INTEGER,
                                   GetIndexDigitVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetStringToH3Function() {
  return CreateScalarFunctionInfo(
      ScalarFunction("h3_string_to_h3", {LogicalType::VARCHAR},
                     LogicalType::UBIGINT, StringToH3Function));
}

CreateScalarFunctionInfo H3Functions::GetH3ToStringFunction() {
  ScalarFunctionSet funcs("h3_h3_to_string");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR,
                                   H3ToStringFunction<uint64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR,
                                   H3ToStringFunction<int64_t>));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetIsValidIndexFunctions() {
  ScalarFunctionSet funcs("h3_is_valid_index");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN,
                                   IsValidIndexVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::BOOLEAN,
                                   IsValidIndexFunction<uint64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BOOLEAN,
                                   IsValidIndexFunction<int64_t>));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetIsValidCellFunctions() {
  ScalarFunctionSet funcs("h3_is_valid_cell");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN,
                                   IsValidCellVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::BOOLEAN,
                                   IsValidCellFunction<uint64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BOOLEAN,
                                   IsValidCellFunction<int64_t>));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetIsResClassIIIFunction() {
  ScalarFunctionSet funcs("h3_is_res_class_iii");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::BOOLEAN,
                                   IsResClassIIIFunction<uint64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BOOLEAN,
                                   IsResClassIIIFunction<int64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN,
                                   IsResClassIIIVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetIsPentagonFunction() {
  ScalarFunctionSet funcs("h3_is_pentagon");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::BOOLEAN,
                                   IsPentagonFunction<uint64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BOOLEAN,
                                   IsPentagonFunction<int64_t>));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN,
                                   IsPentagonVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetIcosahedronFacesFunction() {
  ScalarFunctionSet funcs("h3_get_icosahedron_faces");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
                                   LogicalType::LIST(LogicalType::INTEGER),
                                   GetIcosahedronFacesFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
                                   LogicalType::LIST(LogicalType::INTEGER),
                                   GetIcosahedronFacesFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                   LogicalType::LIST(LogicalType::INTEGER),
                                   GetIcosahedronFacesVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetConstructCellFunction() {
  return CreateScalarFunctionInfo(
      ScalarFunction("h3_construct_cell",
                     {LogicalType::INTEGER, LogicalType::INTEGER,
                      LogicalType::LIST(LogicalType::INTEGER)},
                     LogicalType::UBIGINT, ConstructCellFunction));
}

CreateScalarFunctionInfo H3Functions::GetConstructCellVarcharFunction() {
  return CreateScalarFunctionInfo(
      ScalarFunction("h3_construct_cell_string",
                     {LogicalType::INTEGER, LogicalType::INTEGER,
                      LogicalType::LIST(LogicalType::INTEGER)},
                     LogicalType::VARCHAR, ConstructCellVarcharFunction));
}

} // namespace duckdb
