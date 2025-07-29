#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void GetResolutionFunction(DataChunk &args, ExpressionState &state,
                                  Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<uint64_t, int>(
      inputs, result, args.size(),
      [&](H3Index cell) { return getResolution(cell); });
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

static void GetBaseCellNumberFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<uint64_t, int>(
      inputs, result, args.size(),
      [&](H3Index cell) { return getBaseCellNumber(cell); });
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
    string_t strAsStr = string_t(strdup(str.c_str()), str.size());
    return StringVector::AddString(result, strAsStr);
  }
};

static void H3ToStringFunction(DataChunk &args, ExpressionState &state,
                               Vector &result) {
  UnaryExecutor::ExecuteString<uint64_t, string_t, H3ToStringOperator>(
      args.data[0], result, args.size());
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

static void IsValidCellFunction(DataChunk &args, ExpressionState &state,
                                Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<H3Index, bool>(
      inputs, result, args.size(),
      [&](H3Index input) { return bool(isValidCell(input)); });
}

static void IsResClassIIIFunction(DataChunk &args, ExpressionState &state,
                                  Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<uint64_t, bool>(
      inputs, result, args.size(),
      [&](uint64_t cell) { return bool(isResClassIII(cell)); });
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

static void IsPentagonFunction(DataChunk &args, ExpressionState &state,
                               Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<uint64_t, bool>(
      inputs, result, args.size(),
      [&](uint64_t cell) { return bool(isPentagon(cell)); });
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
  result.Verify(args.size());
}

CreateScalarFunctionInfo H3Functions::GetGetResolutionFunction() {
  ScalarFunctionSet funcs("h3_get_resolution");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::INTEGER,
                                   GetResolutionFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::INTEGER,
                                   GetResolutionFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::INTEGER,
                                   GetResolutionVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetBaseCellNumberFunction() {
  ScalarFunctionSet funcs("h3_get_base_cell_number");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::INTEGER,
                                   GetBaseCellNumberFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::INTEGER,
                                   GetBaseCellNumberFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::INTEGER,
                                   GetBaseCellNumberFunction));
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
                                   H3ToStringFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR,
                                   H3ToStringFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetIsValidCellFunctions() {
  ScalarFunctionSet funcs("h3_is_valid_cell");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN,
                                   IsValidCellVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::BOOLEAN,
                                   IsValidCellFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BOOLEAN,
                                   IsValidCellFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetIsResClassIIIFunction() {
  ScalarFunctionSet funcs("h3_is_res_class_iii");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::BOOLEAN,
                                   IsResClassIIIFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BOOLEAN,
                                   IsResClassIIIFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN,
                                   IsResClassIIIFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetIsPentagonFunction() {
  ScalarFunctionSet funcs("h3_is_pentagon");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::BOOLEAN,
                                   IsPentagonFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BOOLEAN,
                                   IsPentagonFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN,
                                   IsPentagonFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetIcosahedronFacesFunction() {
  ScalarFunctionSet funcs("h3_get_icosahedron_faces");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
                                   LogicalType::LIST(LogicalType::INTEGER),
                                   GetIcosahedronFacesFunction, nullptr, nullptr, nullptr, nullptr,
                                   LogicalType(LogicalTypeId::INVALID),
                                   FunctionStability::VOLATILE));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
                                   LogicalType::LIST(LogicalType::INTEGER),
                                   GetIcosahedronFacesFunction, nullptr, nullptr, nullptr, nullptr,
                                   LogicalType(LogicalTypeId::INVALID),
                                   FunctionStability::VOLATILE));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                   LogicalType::LIST(LogicalType::INTEGER),
                                   GetIcosahedronFacesVarcharFunction, nullptr, nullptr, nullptr, nullptr,
                                   LogicalType(LogicalTypeId::INVALID),
                                   FunctionStability::VOLATILE));
  return CreateScalarFunctionInfo(funcs);
}

} // namespace duckdb
