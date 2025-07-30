#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void LatLngToCellFunction(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  auto &inputs3 = args.data[2];
  TernaryExecutor::ExecuteWithNulls<double, double, int, H3Index>(
      inputs, inputs2, inputs3, result, args.size(),
      [&](double lat, double lng, int res, ValidityMask &mask, idx_t idx) {
        H3Index cell;
        LatLng latLng = {.lat = degsToRads(lat), .lng = degsToRads(lng)};
        H3Error err = latLngToCell(&latLng, res, &cell);
        if (err) {
          mask.SetInvalid(idx);
          return H3Index(H3_NULL);
        } else {
          return cell;
        }
      });
}

static void LatLngToCellVarcharFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  auto &inputs3 = args.data[2];
  TernaryExecutor::ExecuteWithNulls<double, double, int, string_t>(
      inputs, inputs2, inputs3, result, args.size(),
      [&](double lat, double lng, int res, ValidityMask &mask, idx_t idx) {
        H3Index cell;
        LatLng latLng = {.lat = degsToRads(lat), .lng = degsToRads(lng)};
        H3Error err = latLngToCell(&latLng, res, &cell);
        if (err) {
          mask.SetInvalid(idx);
          return StringVector::EmptyString(result, 0);
        } else {
          auto str = StringUtil::Format("%llx", cell);
          string_t strAsStr = string_t(strdup(str.c_str()), str.size());
          return StringVector::AddString(result, strAsStr);
        }
      });
}

static void CellToLatFunction(DataChunk &args, ExpressionState &state,
                              Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<H3Index, double>(
      inputs, result, args.size(),
      [&](H3Index cell, ValidityMask &mask, idx_t idx) {
        LatLng latLng = {.lat = 0, .lng = 0};
        H3Error err = cellToLatLng(cell, &latLng);
        if (err) {
          mask.SetInvalid(idx);
          return .0;
        } else {
          return radsToDegs(latLng.lat);
        }
      });
}

static void CellToLatVarcharFunction(DataChunk &args, ExpressionState &state,
                                     Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<string_t, double>(
      inputs, result, args.size(),
      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
        H3Index cell;
        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
        if (err0) {
          mask.SetInvalid(idx);
          return .0;
        } else {
          LatLng latLng = {.lat = 0, .lng = 0};
          H3Error err = cellToLatLng(cell, &latLng);
          if (err) {
            mask.SetInvalid(idx);
            return .0;
          } else {
            return radsToDegs(latLng.lat);
          }
        }
      });
}

static void CellToLngFunction(DataChunk &args, ExpressionState &state,
                              Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<H3Index, double>(
      inputs, result, args.size(),
      [&](H3Index cell, ValidityMask &mask, idx_t idx) {
        LatLng latLng = {.lat = 0, .lng = 0};
        H3Error err = cellToLatLng(cell, &latLng);
        if (err) {
          mask.SetInvalid(idx);
          return .0;
        } else {
          return radsToDegs(latLng.lng);
        }
      });
}

static void CellToLngVarcharFunction(DataChunk &args, ExpressionState &state,
                                     Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<string_t, double>(
      inputs, result, args.size(),
      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
        H3Index cell;
        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
        if (err0) {
          mask.SetInvalid(idx);
          return .0;
        } else {
          LatLng latLng = {.lat = 0, .lng = 0};
          H3Error err = cellToLatLng(cell, &latLng);
          if (err) {
            mask.SetInvalid(idx);
            return .0;
          } else {
            return radsToDegs(latLng.lng);
          }
        }
      });
}

static void CellToLatLngFunction(DataChunk &args, ExpressionState &state,
                                 Vector &result) {
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    uint64_t cell = args.GetValue(0, i)
                        .DefaultCastAs(LogicalType::UBIGINT)
                        .GetValue<uint64_t>();
    LatLng latLng;
    H3Error err = cellToLatLng(cell, &latLng);
    if (err) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      ListVector::PushBack(result, radsToDegs(latLng.lat));
      ListVector::PushBack(result, radsToDegs(latLng.lng));
      result_data[i].length = 2;
    }
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void CellToLatLngVarcharFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  UnifiedVectorFormat vdata;
  args.data[0].ToUnifiedFormat(args.size(), vdata);

  auto ldata = UnifiedVectorFormat::GetData<string_t>(vdata);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    string_t cellAddress = ldata[i];
    H3Index cell;
    H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
    if (err0) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      LatLng latLng;
      H3Error err = cellToLatLng(cell, &latLng);
      if (err) {
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

struct CellToBoundaryOperator {
  template <class INPUT_TYPE, class RESULT_TYPE>
  static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
    CellBoundary boundary;
    H3Error err = cellToBoundary(input, &boundary);

    if (err) {
      // TODO: Is it possible to return null here instead?
      return StringVector::EmptyString(result, 0);
    } else {
      std::string str = "POLYGON ((";
      for (int i = 0; i <= boundary.numVerts; i++) {
        std::string sep = (i == 0) ? "" : ", ";
        // Add an extra vertex onto the end to close the polygon
        int vertIndex = (i == boundary.numVerts) ? 0 : i;
        str += StringUtil::Format("%s%f %f", sep,
                                  radsToDegs(boundary.verts[vertIndex].lng),
                                  radsToDegs(boundary.verts[vertIndex].lat));
      }
      str += "))";

      return StringVector::AddString(result, str);
    }
  }
};

static void CellToBoundaryWktFunction(DataChunk &args, ExpressionState &state,
                                      Vector &result) {
  UnaryExecutor::ExecuteString<uint64_t, string_t, CellToBoundaryOperator>(
      args.data[0], result, args.size());
}

struct CellToBoundaryVarcharOperator {
  template <class INPUT_TYPE, class RESULT_TYPE>
  static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
    H3Index h;
    H3Error err = stringToH3(input.GetString().c_str(), &h);
    if (err) {
      return StringVector::EmptyString(result, 0);
    } else {
      return CellToBoundaryOperator().Operation<H3Index, RESULT_TYPE>(h,
                                                                      result);
    }
  }
};

static void CellToBoundaryWktVarcharFunction(DataChunk &args,
                                             ExpressionState &state,
                                             Vector &result) {
  UnaryExecutor::ExecuteString<string_t, string_t,
                               CellToBoundaryVarcharOperator>(
      args.data[0], result, args.size());
}

CreateScalarFunctionInfo H3Functions::GetLatLngToCellFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_latlng_to_cell",
      {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::INTEGER},
      LogicalType::UBIGINT, LatLngToCellFunction));
}

CreateScalarFunctionInfo H3Functions::GetLatLngToCellVarcharFunction() {
  return CreateScalarFunctionInfo(ScalarFunction(
      "h3_latlng_to_cell_string",
      {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::INTEGER},
      LogicalType::VARCHAR, LatLngToCellVarcharFunction));
}

CreateScalarFunctionInfo H3Functions::GetCellToLatFunction() {
  ScalarFunctionSet funcs("h3_cell_to_lat");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::DOUBLE,
                                   CellToLatVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::DOUBLE,
                                   CellToLatFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::DOUBLE,
                                   CellToLatFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetCellToLngFunction() {
  ScalarFunctionSet funcs("h3_cell_to_lng");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::DOUBLE,
                                   CellToLngVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::DOUBLE,
                                   CellToLngFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::DOUBLE,
                                   CellToLngFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetCellToLatLngFunction() {
  ScalarFunctionSet funcs("h3_cell_to_latlng");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                   LogicalType::LIST(LogicalType::DOUBLE),
                                   CellToLatLngVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
                                   LogicalType::LIST(LogicalType::DOUBLE),
                                   CellToLatLngFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
                                   LogicalType::LIST(LogicalType::DOUBLE),
                                   CellToLatLngFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetCellToBoundaryWktFunction() {
  ScalarFunctionSet funcs("h3_cell_to_boundary_wkt");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                   CellToBoundaryWktVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR,
                                   CellToBoundaryWktFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR,
                                   CellToBoundaryWktFunction));
  return CreateScalarFunctionInfo(funcs);
}

} // namespace duckdb
