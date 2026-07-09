#include "h3_common.hpp"
#include "h3_functions.hpp"
//#include "well_known_encoder.hpp"

namespace h3duckdb {

void LatLngToCellFunction(duckdb_function_info info, duckdb_data_chunk input,
                          duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector latVec = duckdb_data_chunk_get_vector(input, 0);
  double *latVecData = (double *)duckdb_vector_get_data(latVec);
  uint64_t *latVecValidity = duckdb_vector_get_validity(latVec);
  duckdb_vector lngVec = duckdb_data_chunk_get_vector(input, 1);
  double *lngVecData = (double *)duckdb_vector_get_data(lngVec);
  uint64_t *lngVecValidity = duckdb_vector_get_validity(lngVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 2);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_vector_ensure_validity_writable(output);
  int64_t *resultData = (int64_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    if (duckdb_validity_row_is_valid(latVecValidity, row) &&
        duckdb_validity_row_is_valid(lngVecValidity, row) &&
        duckdb_validity_row_is_valid(resVecValidity, row)) {
      H3Index cell;
      double lat = latVecData[row];
      double lng = lngVecData[row];
      int32_t res = resVecData[row];
      LatLng latLng = {.lat = degsToRads(lat), .lng = degsToRads(lng)};
      H3Error err = latLngToCell(&latLng, res, &cell);
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

void LatLngToCellVarcharFunction(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector latVec = duckdb_data_chunk_get_vector(input, 0);
  double *latVecData = (double *)duckdb_vector_get_data(latVec);
  uint64_t *latVecValidity = duckdb_vector_get_validity(latVec);
  duckdb_vector lngVec = duckdb_data_chunk_get_vector(input, 1);
  double *lngVecData = (double *)duckdb_vector_get_data(lngVec);
  uint64_t *lngVecValidity = duckdb_vector_get_validity(lngVec);
  duckdb_vector resVec = duckdb_data_chunk_get_vector(input, 2);
  int32_t *resVecData = (int32_t *)duckdb_vector_get_data(resVec);
  uint64_t *resVecValidity = duckdb_vector_get_validity(resVec);

  duckdb_vector_ensure_validity_writable(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    bool wasValid = false;

    if (duckdb_validity_row_is_valid(latVecValidity, row) &&
        duckdb_validity_row_is_valid(lngVecValidity, row) &&
        duckdb_validity_row_is_valid(resVecValidity, row)) {
      H3Index cell;
      double lat = latVecData[row];
      double lng = lngVecData[row];
      int32_t res = resVecData[row];
      LatLng latLng = {.lat = degsToRads(lat), .lng = degsToRads(lng)};
      H3Error err = latLngToCell(&latLng, res, &cell);
      if (!err) {
        std::string resultStr = ToHexString(cell);
        duckdb_vector_assign_string_element(output, row, resultStr.c_str());
        wasValid = true;
      }
    }

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}
//
// template <typename T>
// static void CellToLatFunction(DataChunk &args, ExpressionState &state,
//                              Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::ExecuteWithNulls<T, double>(
//      inputs, result, args.size(), [&](T cell, ValidityMask &mask, idx_t idx)
//      {
//        LatLng latLng = {.lat = 0, .lng = 0};
//        H3Error err = cellToLatLng(cell, &latLng);
//        if (err) {
//          mask.SetInvalid(idx);
//          return .0;
//        } else {
//          return radsToDegs(latLng.lat);
//        }
//      });
//}
//
// static void CellToLatVarcharFunction(DataChunk &args, ExpressionState &state,
//                                     Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::ExecuteWithNulls<string_t, double>(
//      inputs, result, args.size(),
//      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
//        H3Index cell;
//        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
//        if (err0) {
//          mask.SetInvalid(idx);
//          return .0;
//        } else {
//          LatLng latLng = {.lat = 0, .lng = 0};
//          H3Error err = cellToLatLng(cell, &latLng);
//          if (err) {
//            mask.SetInvalid(idx);
//            return .0;
//          } else {
//            return radsToDegs(latLng.lat);
//          }
//        }
//      });
//}
//
// template <typename T>
// static void CellToLngFunction(DataChunk &args, ExpressionState &state,
//                              Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::ExecuteWithNulls<T, double>(
//      inputs, result, args.size(), [&](T cell, ValidityMask &mask, idx_t idx)
//      {
//        LatLng latLng = {.lat = 0, .lng = 0};
//        H3Error err = cellToLatLng(cell, &latLng);
//        if (err) {
//          mask.SetInvalid(idx);
//          return .0;
//        } else {
//          return radsToDegs(latLng.lng);
//        }
//      });
//}
//
// static void CellToLngVarcharFunction(DataChunk &args, ExpressionState &state,
//                                     Vector &result) {
//  auto &inputs = args.data[0];
//  UnaryExecutor::ExecuteWithNulls<string_t, double>(
//      inputs, result, args.size(),
//      [&](string_t cellAddress, ValidityMask &mask, idx_t idx) {
//        H3Index cell;
//        H3Error err0 = stringToH3(cellAddress.GetString().c_str(), &cell);
//        if (err0) {
//          mask.SetInvalid(idx);
//          return .0;
//        } else {
//          LatLng latLng = {.lat = 0, .lng = 0};
//          H3Error err = cellToLatLng(cell, &latLng);
//          if (err) {
//            mask.SetInvalid(idx);
//            return .0;
//          } else {
//            return radsToDegs(latLng.lng);
//          }
//        }
//      });
//}
//
// static void CellToLatLngFunction(DataChunk &args, ExpressionState &state,
//                                 Vector &result) {
//  result.SetVectorType(VectorType::FLAT_VECTOR);
//  auto result_data = FlatVector::GetData<list_entry_t>(result);
//  for (idx_t i = 0; i < args.size(); i++) {
//    result_data[i].offset = ListVector::GetListSize(result);
//
//    uint64_t cell = args.GetValue(0, i)
//                        .DefaultCastAs(LogicalType::UBIGINT)
//                        .GetValue<uint64_t>();
//    LatLng latLng;
//    H3Error err = cellToLatLng(cell, &latLng);
//    if (err) {
//      result.SetValue(i, Value(LogicalType::SQLNULL));
//    } else {
//      ListVector::PushBack(result, radsToDegs(latLng.lat));
//      ListVector::PushBack(result, radsToDegs(latLng.lng));
//      result_data[i].length = 2;
//    }
//  }
//  if (args.AllConstant()) {
//    result.SetVectorType(VectorType::CONSTANT_VECTOR);
//  }
//  result.Verify(args.size());
//}
//
// static void CellToLatLngVarcharFunction(DataChunk &args, ExpressionState
// &state,
//                                        Vector &result) {
//  result.SetVectorType(VectorType::FLAT_VECTOR);
//  auto result_data = FlatVector::GetData<list_entry_t>(result);
//  for (idx_t i = 0; i < args.size(); i++) {
//    result_data[i].offset = ListVector::GetListSize(result);
//
//    string cellAddress = args.GetValue(0, i)
//                             .DefaultCastAs(LogicalType::VARCHAR)
//                             .GetValue<string>();
//    H3Index cell;
//    H3Error err0 = stringToH3(cellAddress.c_str(), &cell);
//    if (err0) {
//      result.SetValue(i, Value(LogicalType::SQLNULL));
//    } else {
//      LatLng latLng;
//      H3Error err = cellToLatLng(cell, &latLng);
//      if (err) {
//        result.SetValue(i, Value(LogicalType::SQLNULL));
//      } else {
//        ListVector::PushBack(result, radsToDegs(latLng.lat));
//        ListVector::PushBack(result, radsToDegs(latLng.lng));
//        result_data[i].length = 2;
//      }
//    }
//  }
//  if (args.AllConstant()) {
//    result.SetVectorType(VectorType::CONSTANT_VECTOR);
//  }
//  result.Verify(args.size());
//}
//
// template <typename Encoder> struct CellToBoundaryOperator {
//  explicit CellToBoundaryOperator(Vector &_result) : result(_result) {}
//  string_t operator()(uint64_t input, ValidityMask &mask, idx_t idx) {
//    CellBoundary boundary;
//    H3Error err = cellToBoundary(input, &boundary);
//
//    if (err) {
//      mask.SetInvalid(idx);
//      return StringVector::EmptyString(result, 0);
//    } else {
//      auto enc = Encoder();
//      enc.StartPolygon();
//      for (int i = 0; i <= boundary.numVerts; i++) {
//        // Add an extra vertex onto the end to close the polygon
//        int vertIndex = (i == boundary.numVerts) ? 0 : i;
//        enc.Point(radsToDegs(boundary.verts[vertIndex].lng),
//                  radsToDegs(boundary.verts[vertIndex].lat));
//      }
//      enc.EndPolygon();
//      auto str = enc.Finish();
//
//      return StringVector::AddStringOrBlob(result, str);
//    }
//  }
//
// private:
//  Vector &result;
//};
//
// template <typename T, typename Encoder>
// static void CellToBoundaryFunction(DataChunk &args, ExpressionState &state,
//                                   Vector &result) {
//  UnaryExecutor::ExecuteWithNulls<T, string_t>(
//      args.data[0], result, args.size(),
//      CellToBoundaryOperator<Encoder>{result});
//}
//
// template <typename Encoder> struct CellToBoundaryVarcharOperator {
//  explicit CellToBoundaryVarcharOperator(Vector &_result) : result(_result) {}
//
//  string_t operator()(string_t input, ValidityMask &mask, idx_t idx) {
//    H3Index h;
//    H3Error err = stringToH3(input.GetString().c_str(), &h);
//    if (err) {
//      mask.SetInvalid(idx);
//      return StringVector::EmptyString(result, 0);
//    } else {
//      return CellToBoundaryOperator<Encoder>(result)(h, mask, idx);
//    }
//  }
//
// private:
//  Vector &result;
//};
//
// template <typename Encoder>
// static void CellToBoundaryVarcharFunction(DataChunk &args,
//                                          ExpressionState &state,
//                                          Vector &result) {
//  UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
//      args.data[0], result, args.size(),
//      CellToBoundaryVarcharOperator<Encoder>{result});
//}

duckdb_scalar_function H3Functions::GetLatLngToCellFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_latlng_to_cell");
  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type ubigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_UBIGINT);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, ubigintType);
  duckdb_destroy_logical_type(&doubleType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&ubigintType);
  duckdb_scalar_function_set_function(function, LatLngToCellFunction);
  return function;
}

duckdb_scalar_function H3Functions::GetLatLngToCellVarcharFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_latlng_to_cell_string");
  duckdb_logical_type doubleType =
      duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type stringType =
      duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, doubleType);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, stringType);
  duckdb_destroy_logical_type(&doubleType);
  duckdb_destroy_logical_type(&intType);
  duckdb_destroy_logical_type(&stringType);
  duckdb_scalar_function_set_function(function, LatLngToCellVarcharFunction);
  return function;
}

// CreateScalarFunctionInfo H3Functions::GetCellToLatFunction() {
//  ScalarFunctionSet funcs("h3_cell_to_lat");
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
//  LogicalType::DOUBLE,
//                                   CellToLatVarcharFunction));
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
//  LogicalType::DOUBLE,
//                                   CellToLatFunction<uint64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::DOUBLE,
//                                   CellToLatFunction<int64_t>));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetCellToLngFunction() {
//  ScalarFunctionSet funcs("h3_cell_to_lng");
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
//  LogicalType::DOUBLE,
//                                   CellToLngVarcharFunction));
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
//  LogicalType::DOUBLE,
//                                   CellToLngFunction<uint64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::DOUBLE,
//                                   CellToLngFunction<int64_t>));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetCellToLatLngFunction() {
//  ScalarFunctionSet funcs("h3_cell_to_latlng");
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
//                                   LogicalType::LIST(LogicalType::DOUBLE),
//                                   CellToLatLngVarcharFunction));
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
//                                   LogicalType::LIST(LogicalType::DOUBLE),
//                                   CellToLatLngFunction));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
//                                   LogicalType::LIST(LogicalType::DOUBLE),
//                                   CellToLatLngFunction));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetCellToBoundaryWktFunction() {
//  ScalarFunctionSet funcs("h3_cell_to_boundary_wkt");
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
//  LogicalType::VARCHAR,
//                                   CellToBoundaryVarcharFunction<WktEncoder>));
//  funcs.AddFunction(
//      ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR,
//                     CellToBoundaryFunction<uint64_t, WktEncoder>));
//  funcs.AddFunction(
//      ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR,
//                     CellToBoundaryFunction<int64_t, WktEncoder>));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetCellToBoundaryWkbFunction() {
//  ScalarFunctionSet funcs("h3_cell_to_boundary_wkb");
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BLOB,
//                                   CellToBoundaryVarcharFunction<WkbEncoder>));
//  funcs.AddFunction(
//      ScalarFunction({LogicalType::UBIGINT}, LogicalType::BLOB,
//                     CellToBoundaryFunction<uint64_t, WkbEncoder>));
//  funcs.AddFunction(
//      ScalarFunction({LogicalType::BIGINT}, LogicalType::BLOB,
//                     CellToBoundaryFunction<int64_t, WkbEncoder>));
//  return CreateScalarFunctionInfo(funcs);
//}

} // namespace h3duckdb
