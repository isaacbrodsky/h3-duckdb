#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

// TODO: Consider using enums for (km, m, rads) here, instead of VARCHAR
// (string)

static void GetHexagonAreaAvgFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<int, string_t, double>(
      inputs, inputs2, result, args.size(),
      [&](int res, string_t unit, ValidityMask &mask, idx_t idx) {
        double out;
        H3Error err = E_OPTION_INVALID;
        if (unit == "km^2") {
          err = getHexagonAreaAvgKm2(res, &out);
        } else if (unit == "m^2") {
          err = getHexagonAreaAvgM2(res, &out);
        }
        if (err) {
          mask.SetInvalid(idx);
          return 0.0;
        } else {
          return out;
        }
      });
}

static double CellAreaFunctionInternal(H3Index cell, string_t unit,
                                       ValidityMask &mask, idx_t idx) {
  double out;
  H3Error err = E_OPTION_INVALID;
  if (unit == "rads^2") {
    err = cellAreaRads2(cell, &out);
  } else if (unit == "km^2") {
    err = cellAreaKm2(cell, &out);
  } else if (unit == "m^2") {
    err = cellAreaM2(cell, &out);
  }
  if (err) {
    mask.SetInvalid(idx);
    return 0.0;
  } else {
    return out;
  }
}

static void CellAreaVarcharFunction(DataChunk &args, ExpressionState &state,
                                    Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<string_t, string_t, double>(
      inputs, inputs2, result, args.size(),
      [&](string_t cell, string_t unit, ValidityMask &mask, idx_t idx) {
        H3Index h;
        H3Error err = stringToH3(cell.GetString().c_str(), &h);
        if (err) {
          mask.SetInvalid(idx);
          return 0.0;
        }
        return CellAreaFunctionInternal(h, unit, mask, idx);
      });
}

static void CellAreaFunction(DataChunk &args, ExpressionState &state,
                             Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<H3Index, string_t, double>(
      inputs, inputs2, result, args.size(), CellAreaFunctionInternal);
}

static void GetHexagonEdgeLengthAvgFunction(DataChunk &args,
                                            ExpressionState &state,
                                            Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<int, string_t, double>(
      inputs, inputs2, result, args.size(),
      [&](int res, string_t unit, ValidityMask &mask, idx_t idx) {
        double out;
        H3Error err = E_OPTION_INVALID;
        if (unit == "km") {
          err = getHexagonEdgeLengthAvgKm(res, &out);
        } else if (unit == "m") {
          err = getHexagonEdgeLengthAvgM(res, &out);
        }
        if (err) {
          mask.SetInvalid(idx);
          return 0.0;
        } else {
          return out;
        }
      });
}

static double EdgeLengthFunctionInternal(H3Index edge, string_t unit,
                                         ValidityMask &mask, idx_t idx) {
  double out;
  H3Error err = E_OPTION_INVALID;
  if (unit == "rads") {
    err = edgeLengthRads(edge, &out);
  } else if (unit == "km") {
    err = edgeLengthKm(edge, &out);
  } else if (unit == "m") {
    err = edgeLengthM(edge, &out);
  }
  if (err) {
    mask.SetInvalid(idx);
    return 0.0;
  } else {
    return out;
  }
}

static void EdgeLengthVarcharFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<string_t, string_t, double>(
      inputs, inputs2, result, args.size(),
      [&](string_t edge, string_t unit, ValidityMask &mask, idx_t idx) {
        H3Index h;
        H3Error err = stringToH3(edge.GetString().c_str(), &h);
        if (err) {
          mask.SetInvalid(idx);
          return 0.0;
        }
        return EdgeLengthFunctionInternal(h, unit, mask, idx);
      });
}

static void EdgeLengthFunction(DataChunk &args, ExpressionState &state,
                               Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<H3Index, string_t, double>(
      inputs, inputs2, result, args.size(), EdgeLengthFunctionInternal);
}

static void GetNumCellsFunction(DataChunk &args, ExpressionState &state,
                                Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<int, int64_t>(
      inputs, result, args.size(), [&](int res, ValidityMask &mask, idx_t idx) {
        int64_t out;
        H3Error err = getNumCells(res, &out);
        if (err) {
          mask.SetInvalid(idx);
          return int64_t(0);
        }
        return out;
      });
}

static void GetRes0CellsFunction(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  auto result_data = FlatVector::GetData<list_entry_t>(result);

  int sz = res0CellCount();

  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    std::vector<H3Index> out(sz);
    H3Error err1 = getRes0Cells(out.data());
    if (err1) {
      // This should be unreachable
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      int64_t actual = 0;
      for (auto val : out) {
        if (val != H3_NULL) {
          ListVector::PushBack(result, Value::UBIGINT(val));
          actual++;
        }
      }
      // actual should always be 122

      result_data[i].length = actual;
    }
  }
  result.Verify(args.size());
  result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

static void GetRes0CellsVarcharFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  auto result_data = FlatVector::GetData<list_entry_t>(result);

  int sz = res0CellCount();

  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    std::vector<H3Index> out(sz);
    H3Error err1 = getRes0Cells(out.data());
    if (err1) {
      // This should be unreachable
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      int64_t actual = 0;
      for (auto val : out) {
        if (val != H3_NULL) {
          auto str = StringUtil::Format("%llx", val);
          string_t strAsStr = string_t(strdup(str.c_str()), str.size());
          ListVector::PushBack(result, strAsStr);
          actual++;
        }
      }
      // actual should always be 122

      result_data[i].length = actual;
    }
  }
  result.Verify(args.size());
  result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

static void GetPentagonsFunction(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  auto result_data = FlatVector::GetData<list_entry_t>(result);

  int sz = pentagonCount();

  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    int32_t res = args.GetValue(0, i)
                      .DefaultCastAs(LogicalType::INTEGER)
                      .GetValue<int32_t>();

    std::vector<H3Index> out(sz);
    H3Error err1 = getPentagons(res, out.data());
    if (err1) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      int64_t actual = 0;
      for (auto val : out) {
        if (val != H3_NULL) {
          ListVector::PushBack(result, Value::UBIGINT(val));
          actual++;
        }
      }
      // actual should always be 12

      result_data[i].length = actual;
    }
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void GetPentagonsVarcharFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  auto result_data = FlatVector::GetData<list_entry_t>(result);

  int sz = pentagonCount();

  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    int32_t res = args.GetValue(0, i)
                      .DefaultCastAs(LogicalType::INTEGER)
                      .GetValue<int32_t>();

    std::vector<H3Index> out(sz);
    H3Error err1 = getPentagons(res, out.data());
    if (err1) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      int64_t actual = 0;
      for (auto val : out) {
        if (val != H3_NULL) {
          auto str = StringUtil::Format("%llx", val);
          string_t strAsStr = string_t(strdup(str.c_str()), str.size());
          ListVector::PushBack(result, strAsStr);
          actual++;
        }
      }
      // actual should always be 12

      result_data[i].length = actual;
    }
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void GreatCircleDistanceFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  UnifiedVectorFormat unitData;

  args.data[4].ToUnifiedFormat(args.size(), unitData);

  for (idx_t i = 0; i < args.size(); i++) {
    double dist = 0.0;
    bool isValid = true;

    if (unitData.validity.RowIsValid(i)) {
      double lat0 = args.GetValue(0, i)
                        .DefaultCastAs(LogicalType::DOUBLE)
                        .GetValue<double>();
      double lng0 = args.GetValue(1, i)
                        .DefaultCastAs(LogicalType::DOUBLE)
                        .GetValue<double>();
      double lat1 = args.GetValue(2, i)
                        .DefaultCastAs(LogicalType::DOUBLE)
                        .GetValue<double>();
      double lng1 = args.GetValue(3, i)
                        .DefaultCastAs(LogicalType::DOUBLE)
                        .GetValue<double>();
      string_t unit = args.GetValue(4, i).ToString();

      LatLng latLng0 = {.lat = degsToRads(lat0), .lng = degsToRads(lng0)};
      LatLng latLng1 = {.lat = degsToRads(lat1), .lng = degsToRads(lng1)};

      if (unit == "rads") {
        dist = greatCircleDistanceRads(&latLng0, &latLng1);
      } else if (unit == "km") {
        dist = greatCircleDistanceKm(&latLng0, &latLng1);
      } else if (unit == "m") {
        dist = greatCircleDistanceM(&latLng0, &latLng1);
      } else {
        isValid = false;
      }
    } else {
      isValid = false;
    }

    if (isValid) {
      result.SetValue(i, Value(dist));
    } else {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    }
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

CreateScalarFunctionInfo H3Functions::GetGetHexagonAreaAvgFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_get_hexagon_area_avg", {LogicalType::INTEGER, LogicalType::VARCHAR},
      LogicalType::DOUBLE, GetHexagonAreaAvgFunction));
}

CreateScalarFunctionInfo H3Functions::GetCellAreaFunction() {
  ScalarFunctionSet funcs("h3_cell_area");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   LogicalType::DOUBLE,
                                   CellAreaVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::VARCHAR},
                                   LogicalType::DOUBLE, CellAreaFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::VARCHAR},
                                   LogicalType::DOUBLE, CellAreaFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetHexagonEdgeLengthAvgFunction() {
  return CreateScalarFunctionInfo(
      ScalarFunction("h3_get_hexagon_edge_length_avg",
                     {LogicalType::INTEGER, LogicalType::VARCHAR},
                     LogicalType::DOUBLE, GetHexagonEdgeLengthAvgFunction));
}

CreateScalarFunctionInfo H3Functions::GetEdgeLengthFunction() {
  ScalarFunctionSet funcs("h3_edge_length");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   LogicalType::DOUBLE,
                                   EdgeLengthVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::VARCHAR},
                                   LogicalType::DOUBLE, EdgeLengthFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::VARCHAR},
                                   LogicalType::DOUBLE, EdgeLengthFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetNumCellsFunction() {
  return CreateScalarFunctionInfo(
      ScalarFunction("h3_get_num_cells", {LogicalType::INTEGER},
                     LogicalType::BIGINT, GetNumCellsFunction));
}

CreateScalarFunctionInfo H3Functions::GetGetRes0CellsFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_get_res0_cells", {}, LogicalType::LIST(LogicalType::UBIGINT),
      GetRes0CellsFunction));
}

CreateScalarFunctionInfo H3Functions::GetGetRes0CellsVarcharFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_get_res0_cells_string", {}, LogicalType::LIST(LogicalType::VARCHAR),
      GetRes0CellsVarcharFunction));
}

CreateScalarFunctionInfo H3Functions::GetGetPentagonsFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_get_pentagons", {LogicalType::INTEGER},
      LogicalType::LIST(LogicalType::UBIGINT), GetPentagonsFunction));
}

CreateScalarFunctionInfo H3Functions::GetGetPentagonsVarcharFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_get_pentagons_string", {LogicalType::INTEGER},
      LogicalType::LIST(LogicalType::VARCHAR), GetPentagonsVarcharFunction));
}

CreateScalarFunctionInfo H3Functions::GetGreatCircleDistanceFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_great_circle_distance",
      {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
       LogicalType::DOUBLE, LogicalType::VARCHAR},
      LogicalType::DOUBLE, GreatCircleDistanceFunction));
}

} // namespace duckdb
