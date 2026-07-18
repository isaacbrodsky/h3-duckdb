#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace h3duckdb {

struct GridDiskOperator {
  static H3Error fn(H3Index origin, int32_t k, H3Index *out) {
    return gridDisk(origin, k, out);
  }
};

struct GridDiskUnsafeOperator {
  static H3Error fn(H3Index origin, int32_t k, H3Index *out) {
    return gridDiskUnsafe(origin, k, out);
  }
};

struct GridDiskDistancesOperator {
  static H3Error fn(H3Index origin, int32_t k, H3Index *out,
                    int32_t *distancesOut) {
    return gridDiskDistances(origin, k, out, distancesOut);
  }
};

struct GridDiskDistancesSafeOperator {
  static H3Error fn(H3Index origin, int32_t k, H3Index *out,
                    int32_t *distancesOut) {
    return gridDiskDistancesSafe(origin, k, out, distancesOut);
  }
};

struct GridDiskDistancesUnsafeOperator {
  static H3Error fn(H3Index origin, int32_t k, H3Index *out,
                    int32_t *distancesOut) {
    return gridDiskDistancesUnsafe(origin, k, out, distancesOut);
  }
};

template <typename T, class Operator>
void GridDiskGenericFunction(duckdb_function_info info, duckdb_data_chunk input,
                             duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector kVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *kVecData = (int32_t *)duckdb_vector_get_data(kVec);

  idx_t totalSize = 0;
  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t currentOut = 0;
      H3Error err = maxGridDiskSize(k, &currentOut);

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

    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t sz;
      H3Error err1 = maxGridDiskSize(k, &sz);
      if (!err1) {
        std::vector<H3Index> out(sz);
        H3Error err2 = Operator::fn(parent, k, out.data());
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

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

template <typename T, bool Unsafe>
void GridRingFunction(duckdb_function_info info, duckdb_data_chunk input,
                      duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector kVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *kVecData = (int32_t *)duckdb_vector_get_data(kVec);

  idx_t totalSize = 0;
  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t currentOut = 0;
      H3Error err = maxGridRingSize(k, &currentOut);

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

    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t sz;
      H3Error err1 = maxGridRingSize(k, &sz);
      if (!err1) {
        std::vector<H3Index> out(sz);
        H3Error err2 = Unsafe ? gridRingUnsafe(parent, k, out.data())
                              : gridRing(parent, k, out.data());
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

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

// TODO:
template <typename T, class Operator>
void GridDiskDistancesGenericFunction(duckdb_function_info info,
                                      duckdb_data_chunk input,
                                      duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector indexVec = duckdb_data_chunk_get_vector(input, 0);
  T *indexVecData = (T *)duckdb_vector_get_data(indexVec);
  duckdb_vector kVec = duckdb_data_chunk_get_vector(input, 1);
  int32_t *kVecData = (int32_t *)duckdb_vector_get_data(kVec);

  idx_t totalSize = 0;
  for (idx_t row = 0; row < inputSize; ++row) {
    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t currentOut = 0;
      H3Error err = maxGridDiskSize(k, &currentOut);

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

    H3Index parent = IndexFromVector(indexVecData, row);
    auto k = kVecData[row];

    if (parent) {
      int64_t sz;
      H3Error err1 = maxGridDiskSize(k, &sz);
      if (!err1) {
        std::vector<H3Index> out(sz);
        std::vector<int32_t> distances(sz);
        H3Error err2 = Operator::fn(parent, k, out.data(), distances.data());
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

    if (!wasValid) {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }

  duckdb_list_vector_set_size(output, resultOffset);
}

// template <class Fn>
// static void GridDiskDistancesTmplFunction(DataChunk &args,
//                                          ExpressionState &state,
//                                          Vector &result) {
//  auto result_data = FlatVector::GetData<list_entry_t>(result);
//  for (idx_t i = 0; i < args.size(); i++) {
//    result_data[i].offset = ListVector::GetListSize(result);
//
//    uint64_t origin = args.GetValue(0, i)
//                          .DefaultCastAs(LogicalType::UBIGINT)
//                          .GetValue<uint64_t>();
//    int32_t k = args.GetValue(1, i)
//                    .DefaultCastAs(LogicalType::INTEGER)
//                    .GetValue<int32_t>();
//    int64_t sz;
//    H3Error err1 = maxGridDiskSize(k, &sz);
//    if (err1) {
//      result.SetValue(i, Value(LogicalType::SQLNULL));
//    } else {
//      std::vector<H3Index> out(sz);
//      std::vector<int32_t> distancesOut(sz);
//      H3Error err2 = Fn::fn(origin, k, out.data(), distancesOut.data());
//      if (err2) {
//        result.SetValue(i, Value(LogicalType::SQLNULL));
//      } else {
//        // Reorganize the results similar to H3-Java sorted list of list of
//        // indexes std vector of duckdb vector
//        std::vector<vector<Value>> results(k + 1);
//        for (idx_t j = 0; j < out.size(); j++) {
//          if (out[j] != H3_NULL) {
//            results[distancesOut[j]].push_back(Value::UBIGINT(out[j]));
//          }
//        }
//
//        int64_t actual = 0;
//        for (auto val : results) {
//          ListVector::PushBack(result, Value::LIST(LogicalType::UBIGINT,
//          val)); actual++;
//        }
//
//        result_data[i].length = actual;
//      }
//    }
//  }
//  if (args.AllConstant()) {
//    result.SetVectorType(VectorType::CONSTANT_VECTOR);
//  }
//  result.Verify(args.size());
//}
//

// static void GridPathCellsFunction(DataChunk &args, ExpressionState &state,
//                                  Vector &result) {
//  auto result_data = FlatVector::GetData<list_entry_t>(result);
//  for (idx_t i = 0; i < args.size(); i++) {
//    result_data[i].offset = ListVector::GetListSize(result);
//
//    uint64_t origin = args.GetValue(0, i)
//                          .DefaultCastAs(LogicalType::UBIGINT)
//                          .GetValue<uint64_t>();
//    uint64_t destination = args.GetValue(1, i)
//                               .DefaultCastAs(LogicalType::UBIGINT)
//                               .GetValue<uint64_t>();
//
//    int64_t sz;
//    H3Error err1 = gridPathCellsSize(origin, destination, &sz);
//    if (err1) {
//      result.SetValue(i, Value(LogicalType::SQLNULL));
//    } else {
//      std::vector<H3Index> out(sz);
//      H3Error err2 = gridPathCells(origin, destination, out.data());
//      if (err2) {
//        result.SetValue(i, Value(LogicalType::SQLNULL));
//      } else {
//        int64_t actual = 0;
//        for (auto val : out) {
//          if (val != H3_NULL) {
//            ListVector::PushBack(result, Value::UBIGINT(val));
//            actual++;
//          }
//        }
//
//        result_data[i].length = actual;
//      }
//    }
//  }
//  if (args.AllConstant()) {
//    result.SetVectorType(VectorType::CONSTANT_VECTOR);
//  }
//  result.Verify(args.size());
//}
//
// static void GridPathCellsVarcharFunction(DataChunk &args,
//                                         ExpressionState &state,
//                                         Vector &result) {
//  auto result_data = FlatVector::GetData<list_entry_t>(result);
//  for (idx_t i = 0; i < args.size(); i++) {
//    result_data[i].offset = ListVector::GetListSize(result);
//
//    string originInput = args.GetValue(0, i)
//                             .DefaultCastAs(LogicalType::VARCHAR)
//                             .GetValue<string>();
//    string destinationInput = args.GetValue(1, i)
//                                  .DefaultCastAs(LogicalType::VARCHAR)
//                                  .GetValue<string>();
//
//    H3Index origin, destination;
//    H3Error err0 = stringToH3(originInput.c_str(), &origin);
//    H3Error err1 = stringToH3(destinationInput.c_str(), &destination);
//    if (err0 || err1) {
//      result.SetValue(i, Value(LogicalType::SQLNULL));
//    } else {
//      int64_t sz;
//      H3Error err2 = gridPathCellsSize(origin, destination, &sz);
//      if (err2) {
//        result.SetValue(i, Value(LogicalType::SQLNULL));
//      } else {
//        std::vector<H3Index> out(sz);
//        H3Error err3 = gridPathCells(origin, destination, out.data());
//        if (err3) {
//          result.SetValue(i, Value(LogicalType::SQLNULL));
//        } else {
//          int64_t actual = 0;
//          for (auto val : out) {
//            if (val != H3_NULL) {
//              auto str = StringUtil::Format("%llx", val);
//              ListVector::PushBack(result, str);
//              actual++;
//            }
//          }
//
//          result_data[i].length = actual;
//        }
//      }
//    }
//  }
//  if (args.AllConstant()) {
//    result.SetVectorType(VectorType::CONSTANT_VECTOR);
//  }
//  result.Verify(args.size());
//}
//
// template <typename T>
// static void GridDistanceFunction(DataChunk &args, ExpressionState &state,
//                                 Vector &result) {
//  auto &inputs = args.data[0];
//  auto &inputs2 = args.data[1];
//  BinaryExecutor::ExecuteWithNulls<T, T, int64_t>(
//      inputs, inputs2, result, args.size(),
//      [&](T origin, T destination, ValidityMask &mask, idx_t idx) {
//        int64_t distance;
//        H3Error err = gridDistance(origin, destination, &distance);
//        if (err) {
//          mask.SetInvalid(idx);
//          return int64_t(0);
//        } else {
//          return distance;
//        }
//      });
//}
//
// static void GridDistanceVarcharFunction(DataChunk &args, ExpressionState
// &state,
//                                        Vector &result) {
//  auto &inputs = args.data[0];
//  auto &inputs2 = args.data[1];
//  BinaryExecutor::ExecuteWithNulls<string_t, string_t, int64_t>(
//      inputs, inputs2, result, args.size(),
//      [&](string_t originInput, string_t destinationInput, ValidityMask &mask,
//          idx_t idx) {
//        H3Index origin, destination;
//        H3Error err0 = stringToH3(originInput.GetString().c_str(), &origin);
//        H3Error err1 =
//            stringToH3(destinationInput.GetString().c_str(), &destination);
//        if (err0 || err1) {
//          mask.SetInvalid(idx);
//          return int64_t(0);
//        } else {
//          int64_t distance;
//          H3Error err = gridDistance(origin, destination, &distance);
//          if (err) {
//            mask.SetInvalid(idx);
//            return int64_t(0);
//          } else {
//            return distance;
//          }
//        }
//      });
//}
//
// static void CellToLocalIjFunction(DataChunk &args, ExpressionState &state,
//                                  Vector &result) {
//  auto result_data = FlatVector::GetData<list_entry_t>(result);
//  for (idx_t i = 0; i < args.size(); i++) {
//    result_data[i].offset = ListVector::GetListSize(result);
//
//    uint64_t origin = args.GetValue(0, i)
//                          .DefaultCastAs(LogicalType::UBIGINT)
//                          .GetValue<uint64_t>();
//    uint64_t cell = args.GetValue(1, i)
//                        .DefaultCastAs(LogicalType::UBIGINT)
//                        .GetValue<uint64_t>();
//    uint32_t mode = 0; // TODO: Expose mode to the user when applicable
//
//    CoordIJ out;
//    H3Error err = cellToLocalIj(origin, cell, mode, &out);
//    if (err) {
//      result.SetValue(i, Value(LogicalType::SQLNULL));
//    } else {
//      ListVector::PushBack(result, Value::INTEGER(out.i));
//      ListVector::PushBack(result, Value::INTEGER(out.j));
//      result_data[i].length = 2;
//    }
//  }
//  if (args.AllConstant()) {
//    result.SetVectorType(VectorType::CONSTANT_VECTOR);
//  }
//  result.Verify(args.size());
//}
//
// static void CellToLocalIjVarcharFunction(DataChunk &args,
//                                         ExpressionState &state,
//                                         Vector &result) {
//  auto result_data = FlatVector::GetData<list_entry_t>(result);
//  for (idx_t i = 0; i < args.size(); i++) {
//    result_data[i].offset = ListVector::GetListSize(result);
//
//    string originInput = args.GetValue(0, i)
//                             .DefaultCastAs(LogicalType::VARCHAR)
//                             .GetValue<string>();
//    string cellInput = args.GetValue(1, i)
//                           .DefaultCastAs(LogicalType::VARCHAR)
//                           .GetValue<string>();
//    uint32_t mode = 0; // TODO: Expose mode to the user when applicable
//
//    H3Index origin, cell;
//    H3Error err0 = stringToH3(originInput.c_str(), &origin);
//    H3Error err1 = stringToH3(cellInput.c_str(), &cell);
//    if (err0 || err1) {
//      result.SetValue(i, Value(LogicalType::SQLNULL));
//    } else {
//      CoordIJ out;
//      H3Error err2 = cellToLocalIj(origin, cell, mode, &out);
//      if (err2) {
//        result.SetValue(i, Value(LogicalType::SQLNULL));
//      } else {
//        ListVector::PushBack(result, Value::INTEGER(out.i));
//        ListVector::PushBack(result, Value::INTEGER(out.j));
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
// template <typename T>
// static void LocalIjToCellFunction(DataChunk &args, ExpressionState &state,
//                                  Vector &result) {
//  auto &inputs = args.data[0];
//  auto &inputs2 = args.data[1];
//  auto &inputs3 = args.data[2];
//  TernaryExecutor::ExecuteWithNulls<T, int32_t, int32_t, T>(
//      inputs, inputs2, inputs3, result, args.size(),
//      [&](T origin, int32_t i, int32_t j, ValidityMask &mask, idx_t idx) {
//        uint32_t mode = 0; // TODO: Expose mode to the user when applicable
//
//        CoordIJ coordIJ{.i = i, .j = j};
//        H3Index out;
//        H3Error err = localIjToCell(origin, &coordIJ, mode, &out);
//        if (err) {
//          mask.SetInvalid(idx);
//          return H3Index(H3_NULL);
//        } else {
//          return out;
//        }
//      });
//}
//
// static void LocalIjToCellVarcharFunction(DataChunk &args,
//                                         ExpressionState &state,
//                                         Vector &result) {
//  auto &inputs = args.data[0];
//  auto &inputs2 = args.data[1];
//  auto &inputs3 = args.data[2];
//  TernaryExecutor::ExecuteWithNulls<string_t, int32_t, int32_t, string_t>(
//      inputs, inputs2, inputs3, result, args.size(),
//      [&](string_t input, int32_t i, int32_t j, ValidityMask &mask, idx_t idx)
//      {
//        H3Index origin;
//        H3Error err0 = stringToH3(input.GetString().c_str(), &origin);
//        if (err0) {
//          mask.SetInvalid(idx);
//          return StringVector::EmptyString(result, 0);
//        } else {
//          uint32_t mode = 0; // TODO: Expose mode to the user when applicable
//
//          CoordIJ coordIJ{.i = i, .j = j};
//          H3Index out;
//          H3Error err1 = localIjToCell(origin, &coordIJ, mode, &out);
//          if (err1) {
//            mask.SetInvalid(idx);
//            return StringVector::EmptyString(result, 0);
//          } else {
//            auto str = StringUtil::Format("%llx", out);
//            return StringVector::AddString(result, str);
//          }
//        }
//      });
//}

void MaxGridDiskSizeFunction(duckdb_function_info info, duckdb_data_chunk input,
                             duckdb_vector output) {
  idx_t inputSize = duckdb_data_chunk_get_size(input);

  duckdb_vector kVec = duckdb_data_chunk_get_vector(input, 0);
  int32_t *kVecData = (int32_t *)duckdb_vector_get_data(kVec);

  duckdb_vector_ensure_validity_writable(output);
  int64_t *resultData = (int64_t *)duckdb_vector_get_data(output);
  uint64_t *resultValidity = duckdb_vector_get_validity(output);

  for (idx_t row = 0; row < inputSize; ++row) {
    auto k = kVecData[row];
    int64_t out = 0;

    H3Error err = maxGridDiskSize(k, &out);
    if (!err) {
      resultData[row] = out;
    } else {
      duckdb_validity_set_row_invalid(resultValidity, row);
    }
  }
}

duckdb_scalar_function_set H3Functions::GetGridDiskFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_disk");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_disk");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalListType);
    duckdb_scalar_function_set_function(
        function, GridDiskGenericFunction<PhysicalType, GridDiskOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

duckdb_scalar_function H3Functions::GetMaxGridDiskSizeFunction() {
  duckdb_scalar_function function = duckdb_create_scalar_function();
  duckdb_scalar_function_set_name(function, "h3_max_grid_disk_size");
  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
  duckdb_logical_type bigintType =
      duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
  duckdb_scalar_function_add_parameter(function, intType);
  duckdb_scalar_function_set_return_type(function, bigintType);
  duckdb_destroy_logical_type(&bigintType);
  duckdb_scalar_function_set_function(function, MaxGridDiskSizeFunction);
  return function;
}

// CreateScalarFunctionInfo H3Functions::GetGridDiskDistancesFunction() {
//  ScalarFunctionSet funcs("h3_grid_disk_distances");
//  funcs.AddFunction(
//      ScalarFunction({LogicalType::UBIGINT, LogicalType::INTEGER},
//                     LogicalType::LIST(LogicalType::LIST(LogicalType::UBIGINT)),
//                     GridDiskDistancesTmplFunction<GridDiskDistancesOperator>));
//  funcs.AddFunction(
//      ScalarFunction({LogicalType::BIGINT, LogicalType::INTEGER},
//                     LogicalType::LIST(LogicalType::LIST(LogicalType::BIGINT)),
//                     GridDiskDistancesTmplFunction<GridDiskDistancesOperator>));
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::VARCHAR, LogicalType::INTEGER},
//      LogicalType::LIST(LogicalType::LIST(LogicalType::VARCHAR)),
//      GridDiskDistancesTmplVarcharFunction<GridDiskDistancesOperator>));
//  return CreateScalarFunctionInfo(funcs);
//}
//

duckdb_scalar_function_set H3Functions::GetGridDiskUnsafeFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_disk_unsafe");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_disk_unsafe");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalListType);
    duckdb_scalar_function_set_function(
        function,
        GridDiskGenericFunction<PhysicalType, GridDiskUnsafeOperator>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}
//
// CreateScalarFunctionInfo H3Functions::GetGridDiskDistancesUnsafeFunction() {
//  ScalarFunctionSet funcs("h3_grid_disk_distances_unsafe");
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::UBIGINT, LogicalType::INTEGER},
//      LogicalType::LIST(LogicalType::LIST(LogicalType::UBIGINT)),
//      GridDiskDistancesTmplFunction<GridDiskDistancesUnsafeOperator>));
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::BIGINT, LogicalType::INTEGER},
//      LogicalType::LIST(LogicalType::LIST(LogicalType::BIGINT)),
//      GridDiskDistancesTmplFunction<GridDiskDistancesUnsafeOperator>));
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::VARCHAR, LogicalType::INTEGER},
//      LogicalType::LIST(LogicalType::LIST(LogicalType::VARCHAR)),
//      GridDiskDistancesTmplVarcharFunction<GridDiskDistancesUnsafeOperator>));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetGridDiskDistancesSafeFunction() {
//  ScalarFunctionSet funcs("h3_grid_disk_distances_safe");
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::UBIGINT, LogicalType::INTEGER},
//      LogicalType::LIST(LogicalType::LIST(LogicalType::UBIGINT)),
//      GridDiskDistancesTmplFunction<GridDiskDistancesSafeOperator>));
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::BIGINT, LogicalType::INTEGER},
//      LogicalType::LIST(LogicalType::LIST(LogicalType::BIGINT)),
//      GridDiskDistancesTmplFunction<GridDiskDistancesSafeOperator>));
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::VARCHAR, LogicalType::INTEGER},
//      LogicalType::LIST(LogicalType::LIST(LogicalType::VARCHAR)),
//      GridDiskDistancesTmplVarcharFunction<GridDiskDistancesSafeOperator>));
//  return CreateScalarFunctionInfo(funcs);
//}
//
duckdb_scalar_function_set H3Functions::GetGridRingFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_ring");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_ring");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalListType);
    duckdb_scalar_function_set_function(function,
                                        GridRingFunction<PhysicalType, false>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}
duckdb_scalar_function_set H3Functions::GetGridRingUnsafeFunction() {
  duckdb_scalar_function_set functionSet =
      duckdb_create_scalar_function_set("h3_grid_ring_unsafe");

  duckdb_logical_type intType = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);

  auto r = [&functionSet, &intType]<typename PhysicalType>(duckdb_type typeId) {
    auto logicalType = duckdb_create_logical_type(typeId);
    auto logicalListType = duckdb_create_list_type(logicalType);

    duckdb_scalar_function function = duckdb_create_scalar_function();
    duckdb_scalar_function_set_name(function, "h3_grid_ring_unsafe");
    duckdb_scalar_function_add_parameter(function, logicalType);
    duckdb_scalar_function_add_parameter(function, intType);
    duckdb_scalar_function_set_return_type(function, logicalListType);
    duckdb_scalar_function_set_function(function,
                                        GridRingFunction<PhysicalType, true>);
    duckdb_add_scalar_function_to_set(functionSet, function);
    duckdb_destroy_scalar_function(&function);

    duckdb_destroy_logical_type(&logicalType);
  };

  r.operator()<uint64_t>(DUCKDB_TYPE_UBIGINT);
  r.operator()<int64_t>(DUCKDB_TYPE_BIGINT);
  r.operator()<duckdb_string_t>(DUCKDB_TYPE_VARCHAR);

  duckdb_destroy_logical_type(&intType);

  return functionSet;
}

// CreateScalarFunctionInfo H3Functions::GetGridPathCellsFunction() {
//  ScalarFunctionSet funcs("h3_grid_path_cells");
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT,
//  LogicalType::UBIGINT},
//                                   LogicalType::LIST(LogicalType::UBIGINT),
//                                   GridPathCellsFunction));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT},
//                                   LogicalType::LIST(LogicalType::BIGINT),
//                                   GridPathCellsFunction));
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR,
//  LogicalType::VARCHAR},
//                                   LogicalType::LIST(LogicalType::VARCHAR),
//                                   GridPathCellsVarcharFunction));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetGridDistanceFunction() {
//  ScalarFunctionSet funcs("h3_grid_distance");
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT,
//  LogicalType::UBIGINT},
//                                   LogicalType::BIGINT,
//                                   GridDistanceFunction<uint64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT},
//                                   LogicalType::BIGINT,
//                                   GridDistanceFunction<int64_t>));
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR,
//  LogicalType::VARCHAR},
//                                   LogicalType::BIGINT,
//                                   GridDistanceVarcharFunction));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetCellToLocalIjFunction() {
//  ScalarFunctionSet funcs("h3_cell_to_local_ij");
//  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT,
//  LogicalType::UBIGINT},
//                                   LogicalType::LIST(LogicalType::INTEGER),
//                                   CellToLocalIjFunction));
//  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT},
//                                   LogicalType::LIST(LogicalType::INTEGER),
//                                   CellToLocalIjFunction));
//  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR,
//  LogicalType::VARCHAR},
//                                   LogicalType::LIST(LogicalType::VARCHAR),
//                                   CellToLocalIjVarcharFunction));
//  return CreateScalarFunctionInfo(funcs);
//}
//
// CreateScalarFunctionInfo H3Functions::GetLocalIjToCellFunction() {
//  ScalarFunctionSet funcs("h3_local_ij_to_cell");
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::UBIGINT, LogicalType::INTEGER, LogicalType::INTEGER},
//      LogicalType::UBIGINT, LocalIjToCellFunction<uint64_t>));
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::BIGINT, LogicalType::INTEGER, LogicalType::INTEGER},
//      LogicalType::BIGINT, LocalIjToCellFunction<int64_t>));
//  funcs.AddFunction(ScalarFunction(
//      {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER},
//      LogicalType::VARCHAR, LocalIjToCellVarcharFunction));
//  return CreateScalarFunctionInfo(funcs);
//}

} // namespace h3duckdb
