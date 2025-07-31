#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void CellToVertexFunction(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<H3Index, int32_t, H3Index>(
      inputs, inputs2, result, args.size(),
      [&](H3Index cell, int32_t vertexNum, ValidityMask &mask, idx_t idx) {
        H3Index vertex;
        H3Error err = cellToVertex(cell, vertexNum, &vertex);
        if (err) {
          mask.SetInvalid(idx);
          return H3Index(H3_NULL);
        } else {
          return vertex;
        }
      });
}

static void CellToVertexVarcharFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<string_t, int32_t, string_t>(
      inputs, inputs2, result, args.size(),
      [&](string_t cellInput, int32_t vertexNum, ValidityMask &mask,
          idx_t idx) {
        H3Index cell;
        H3Error err0 = stringToH3(cellInput.GetString().c_str(), &cell);
        if (err0) {
          mask.SetInvalid(idx);
          return StringVector::EmptyString(result, 0);
        } else {
          H3Index vertex;
          H3Error err1 = cellToVertex(cell, vertexNum, &vertex);
          if (err1) {
            mask.SetInvalid(idx);
            return StringVector::EmptyString(result, 0);
          } else {
            auto str = StringUtil::Format("%llx", vertex);
            string_t strAsStr = string_t(strdup(str.c_str()), str.size());
            return StringVector::AddString(result, strAsStr);
          }
        }
      });
}

static void CellToVertexesFunction(DataChunk &args, ExpressionState &state,
                                   Vector &result) {
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto &result_validity = FlatVector::Validity(result);
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  idx_t offset = 0;
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = offset;

    uint64_t cell = args.GetValue(0, i)
                        .DefaultCastAs(LogicalType::UBIGINT)
                        .GetValue<uint64_t>();

    int64_t actual = 0;
    std::vector<H3Index> out(6);
    H3Error err = cellToVertexes(cell, out.data());
    if (err) {
      result_validity.SetInvalid(i);
      result_data[i].length = 0;
    } else {
      for (auto val : out) {
        if (val != H3_NULL) {
          auto result_val = Value::UBIGINT(val);
          ListVector::PushBack(result, result_val);
          actual++;
        }
      }

      result_data[i].length = actual;
    }
    offset += actual;
  }

  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void CellToVertexesVarcharFunction(DataChunk &args,
                                          ExpressionState &state,
                                          Vector &result) {
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto &result_validity = FlatVector::Validity(result);
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  idx_t offset = 0;
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = offset;

    string cellInput = args.GetValue(0, i)
                           .DefaultCastAs(LogicalType::VARCHAR)
                           .GetValue<string>();

    H3Index cell;
    H3Error err0 = stringToH3(cellInput.c_str(), &cell);
    if (err0) {
      result_validity.SetInvalid(i);
      result_data[i].length = 0;
    } else {
      int64_t actual = 0;
      std::vector<H3Index> out(6);
      H3Error err = cellToVertexes(cell, out.data());
      if (err) {
        result_validity.SetInvalid(i);
        result_data[i].length = 0;
      } else {
        for (auto val : out) {
          if (val != H3_NULL) {
            auto str = StringUtil::Format("%llx", val);
            string_t strAsStr = string_t(strdup(str.c_str()), str.size());
            ListVector::PushBack(result, strAsStr);
            actual++;
          }
        }

        result_data[i].length = actual;
      }
      offset += actual;
    }
  }

  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void VertexToLatFunction(DataChunk &args, ExpressionState &state,
                                Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<H3Index, double>(
      inputs, result, args.size(),
      [&](H3Index vertex, ValidityMask &mask, idx_t idx) {
        LatLng latLng = {.lat = 0, .lng = 0};
        H3Error err = vertexToLatLng(vertex, &latLng);
        if (err) {
          mask.SetInvalid(idx);
          return .0;
        } else {
          return radsToDegs(latLng.lat);
        }
      });
}

static void VertexToLatVarcharFunction(DataChunk &args, ExpressionState &state,
                                       Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<string_t, double>(
      inputs, result, args.size(),
      [&](string_t vertexInput, ValidityMask &mask, idx_t idx) {
        H3Index vertex;
        H3Error err0 = stringToH3(vertexInput.GetString().c_str(), &vertex);
        if (err0) {
          mask.SetInvalid(idx);
          return .0;
        } else {
          LatLng latLng = {.lat = 0, .lng = 0};
          H3Error err1 = vertexToLatLng(vertex, &latLng);
          if (err1) {
            mask.SetInvalid(idx);
            return .0;
          } else {
            return radsToDegs(latLng.lat);
          }
        }
      });
}

static void VertexToLngFunction(DataChunk &args, ExpressionState &state,
                                Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<H3Index, double>(
      inputs, result, args.size(),
      [&](H3Index vertex, ValidityMask &mask, idx_t idx) {
        LatLng latLng = {.lat = 0, .lng = 0};
        H3Error err = vertexToLatLng(vertex, &latLng);
        if (err) {
          mask.SetInvalid(idx);
          return .0;
        } else {
          return radsToDegs(latLng.lng);
        }
      });
}

static void VertexToLngVarcharFunction(DataChunk &args, ExpressionState &state,
                                       Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<string_t, double>(
      inputs, result, args.size(),
      [&](string_t vertexInput, ValidityMask &mask, idx_t idx) {
        H3Index vertex;
        H3Error err0 = stringToH3(vertexInput.GetString().c_str(), &vertex);
        if (err0) {
          mask.SetInvalid(idx);
          return .0;
        } else {
          LatLng latLng = {.lat = 0, .lng = 0};
          H3Error err1 = vertexToLatLng(vertex, &latLng);
          if (err1) {
            mask.SetInvalid(idx);
            return .0;
          } else {
            return radsToDegs(latLng.lng);
          }
        }
      });
}

static void VertexToLatLngFunction(DataChunk &args, ExpressionState &state,
                                   Vector &result) {
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    uint64_t vertex = args.GetValue(0, i)
                          .DefaultCastAs(LogicalType::UBIGINT)
                          .GetValue<uint64_t>();
    LatLng latLng;
    H3Error err = vertexToLatLng(vertex, &latLng);
    ThrowH3Error(err);

    ListVector::PushBack(result, radsToDegs(latLng.lat));
    ListVector::PushBack(result, radsToDegs(latLng.lng));
    result_data[i].length = 2;
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void VertexToLatLngVarcharFunction(DataChunk &args,
                                          ExpressionState &state,
                                          Vector &result) {
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    string vertexInput = args.GetValue(0, i)
                             .DefaultCastAs(LogicalType::VARCHAR)
                             .GetValue<string>();
    H3Index vertex;
    H3Error err0 = stringToH3(vertexInput.c_str(), &vertex);
    if (err0) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      LatLng latLng;
      H3Error err1 = vertexToLatLng(vertex, &latLng);
      if (err1) {
        result.SetValue(i, Value(LogicalType::SQLNULL));
      } else {
        ListVector::PushBack(result, radsToDegs(latLng.lat));
        ListVector::PushBack(result, radsToDegs(latLng.lng));
        result_data[i].length = 2;
      }
    }
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void IsValidVertexVarcharFunction(DataChunk &args,
                                         ExpressionState &state,
                                         Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<string_t, bool>(
      inputs, result, args.size(), [&](string_t input) {
        H3Index h;
        H3Error err = stringToH3(input.GetString().c_str(), &h);
        if (err) {
          return false;
        }
        return bool(isValidVertex(h));
      });
}

static void IsValidVertexFunction(DataChunk &args, ExpressionState &state,
                                  Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<H3Index, bool>(
      inputs, result, args.size(),
      [&](H3Index input) { return bool(isValidVertex(input)); });
}

CreateScalarFunctionInfo H3Functions::GetCellToVertexFunction() {
  ScalarFunctionSet funcs("h3_cell_to_vertex");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::INTEGER},
                                   LogicalType::UBIGINT, CellToVertexFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER},
                                   LogicalType::BIGINT, CellToVertexFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER},
                                   LogicalType::VARCHAR,
                                   CellToVertexVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetCellToVertexesFunction() {
  ScalarFunctionSet funcs("h3_cell_to_vertexes");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
                                   LogicalType::LIST(LogicalType::UBIGINT),
                                   CellToVertexesFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
                                   LogicalType::LIST(LogicalType::BIGINT),
                                   CellToVertexesFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                   LogicalType::LIST(LogicalType::VARCHAR),
                                   CellToVertexesVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetVertexToLatFunction() {
  ScalarFunctionSet funcs("h3_vertex_to_lat");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::DOUBLE,
                                   VertexToLatFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::DOUBLE,
                                   VertexToLatFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::DOUBLE,
                                   VertexToLatVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetVertexToLngFunction() {
  ScalarFunctionSet funcs("h3_vertex_to_lng");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::DOUBLE,
                                   VertexToLngFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::DOUBLE,
                                   VertexToLngFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::DOUBLE,
                                   VertexToLngVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetVertexToLatLngFunction() {
  ScalarFunctionSet funcs("h3_vertex_to_latlng");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
                                   LogicalType::LIST(LogicalType::DOUBLE),
                                   VertexToLatLngFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
                                   LogicalType::LIST(LogicalType::DOUBLE),
                                   VertexToLatLngFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                   LogicalType::LIST(LogicalType::DOUBLE),
                                   VertexToLatLngVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetIsValidVertexFunctions() {
  ScalarFunctionSet funcs("h3_is_valid_vertex");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN,
                                   IsValidVertexVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::BOOLEAN,
                                   IsValidVertexFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BOOLEAN,
                                   IsValidVertexFunction));
  return CreateScalarFunctionInfo(funcs);
}

} // namespace duckdb
