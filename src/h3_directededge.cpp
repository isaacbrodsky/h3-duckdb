#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void DirectedEdgeToCellsFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    uint64_t edge = args.GetValue(0, i)
                        .DefaultCastAs(LogicalType::UBIGINT)
                        .GetValue<uint64_t>();
    std::vector<H3Index> out(2);
    H3Error err = directedEdgeToCells(edge, out.data());
    if (err) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      ListVector::PushBack(result, Value::UBIGINT(out[0]));
      ListVector::PushBack(result, Value::UBIGINT(out[1]));

      result_data[i].length = 2;
    }
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void DirectedEdgeToCellsVarcharFunction(DataChunk &args,
                                               ExpressionState &state,
                                               Vector &result) {
  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    string edgeInput = args.GetValue(0, i)
                           .DefaultCastAs(LogicalType::VARCHAR)
                           .GetValue<string>();

    H3Index edge;
    H3Error err0 = stringToH3(edgeInput.c_str(), &edge);
    if (err0) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      std::vector<H3Index> out(2);
      H3Error err1 = directedEdgeToCells(edge, out.data());
      if (err1) {
        result.SetValue(i, Value(LogicalType::SQLNULL));
      } else {
        auto str0 = StringUtil::Format("%llx", out[0]);
        string_t strAsStr0 = string_t(strdup(str0.c_str()), str0.size());
        ListVector::PushBack(result, strAsStr0);
        auto str1 = StringUtil::Format("%llx", out[1]);
        string_t strAsStr1 = string_t(strdup(str1.c_str()), str1.size());
        ListVector::PushBack(result, strAsStr1);

        result_data[i].length = 2;
      }
    }
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void OriginToDirectedEdgesFunction(DataChunk &args,
                                          ExpressionState &state,
                                          Vector &result) {
  D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    uint64_t origin = args.GetValue(0, i)
                          .DefaultCastAs(LogicalType::UBIGINT)
                          .GetValue<uint64_t>();
    int64_t actual = 0;
    std::vector<H3Index> out(6);
    H3Error err = originToDirectedEdges(origin, out.data());
    if (err) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      for (auto val : out) {
        if (val != H3_NULL) {
          ListVector::PushBack(result, Value::UBIGINT(val));
          actual++;
        }
      }

      result_data[i].length = actual;
    }
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void OriginToDirectedEdgesVarcharFunction(DataChunk &args,
                                                 ExpressionState &state,
                                                 Vector &result) {
  D_ASSERT(result.GetType().id() == LogicalTypeId::LIST);

  auto result_data = FlatVector::GetData<list_entry_t>(result);
  for (idx_t i = 0; i < args.size(); i++) {
    result_data[i].offset = ListVector::GetListSize(result);

    string originStr = args.GetValue(0, i)
                           .DefaultCastAs(LogicalType::VARCHAR)
                           .GetValue<string>();

    H3Index origin;
    H3Error err0 = stringToH3(originStr.c_str(), &origin);
    if (err0) {
      result.SetValue(i, Value(LogicalType::SQLNULL));
    } else {
      int64_t actual = 0;
      std::vector<H3Index> out(6);
      H3Error err1 = originToDirectedEdges(origin, out.data());
      if (err1) {
        result.SetValue(i, Value(LogicalType::SQLNULL));
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
    }
  }
  if (args.AllConstant()) {
    result.SetVectorType(VectorType::CONSTANT_VECTOR);
  }
  result.Verify(args.size());
}

static void GetDirectedEdgeOriginFunction(DataChunk &args,
                                          ExpressionState &state,
                                          Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<uint64_t, uint64_t>(
      inputs, result, args.size(),
      [&](uint64_t input, ValidityMask &mask, idx_t idx) {
        H3Index out;
        H3Error err = getDirectedEdgeOrigin(input, &out);
        if (err) {
          mask.SetInvalid(idx);
          return H3Index(H3_NULL);
        } else {
          return out;
        }
      });
}

static void GetDirectedEdgeOriginVarcharFunction(DataChunk &args,
                                                 ExpressionState &state,
                                                 Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
      inputs, result, args.size(),
      [&](string_t inputStr, ValidityMask &mask, idx_t idx) {
        H3Index input;
        H3Error err0 = stringToH3(inputStr.GetString().c_str(), &input);
        if (err0) {
          mask.SetInvalid(idx);
          return StringVector::EmptyString(result, 0);
        } else {
          H3Index out;
          H3Error err1 = getDirectedEdgeOrigin(input, &out);
          if (err1) {
            mask.SetInvalid(idx);
            return StringVector::EmptyString(result, 0);
          } else {
            auto str = StringUtil::Format("%llx", out);
            string_t strAsStr = string_t(strdup(str.c_str()), str.size());
            return StringVector::AddString(result, strAsStr);
          }
        }
      });
}

static void GetDirectedEdgeDestinationFunction(DataChunk &args,
                                               ExpressionState &state,
                                               Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<uint64_t, uint64_t>(
      inputs, result, args.size(),
      [&](uint64_t input, ValidityMask &mask, idx_t idx) {
        H3Index out;
        H3Error err = getDirectedEdgeDestination(input, &out);
        if (err) {
          mask.SetInvalid(idx);
          return H3Index(H3_NULL);
        } else {
          return out;
        }
      });
}

static void GetDirectedEdgeDestinationVarcharFunction(DataChunk &args,
                                                      ExpressionState &state,
                                                      Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
      inputs, result, args.size(),
      [&](string_t inputStr, ValidityMask &mask, idx_t idx) {
        H3Index input;
        H3Error err0 = stringToH3(inputStr.GetString().c_str(), &input);
        if (err0) {
          mask.SetInvalid(idx);
          return StringVector::EmptyString(result, 0);
        } else {
          H3Index out;
          H3Error err1 = getDirectedEdgeDestination(input, &out);
          if (err1) {
            mask.SetInvalid(idx);
            return StringVector::EmptyString(result, 0);
          } else {
            auto str = StringUtil::Format("%llx", out);
            string_t strAsStr = string_t(strdup(str.c_str()), str.size());
            return StringVector::AddString(result, strAsStr);
          }
        }
      });
}

static void CellsToDirectedEdgeFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<uint64_t, uint64_t, uint64_t>(
      inputs, inputs2, result, args.size(),
      [&](uint64_t input, uint64_t input2, ValidityMask &mask, idx_t idx) {
        H3Index out;
        H3Error err = cellsToDirectedEdge(input, input2, &out);
        if (err) {
          mask.SetInvalid(idx);
          return H3Index(H3_NULL);
        } else {
          return out;
        }
      });
}

static void CellsToDirectedEdgeVarcharFunction(DataChunk &args,
                                               ExpressionState &state,
                                               Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<string_t, string_t, string_t>(
      inputs, inputs2, result, args.size(),
      [&](string_t inputStr, string_t inputStr2, ValidityMask &mask,
          idx_t idx) {
        H3Index input, input2;
        H3Error err0 = stringToH3(inputStr.GetString().c_str(), &input);
        H3Error err1 = stringToH3(inputStr2.GetString().c_str(), &input2);
        if (err0 || err1) {
          mask.SetInvalid(idx);
          return StringVector::EmptyString(result, 0);
        } else {
          H3Index out;
          H3Error err = cellsToDirectedEdge(input, input2, &out);
          if (err) {
            mask.SetInvalid(idx);
            return StringVector::EmptyString(result, 0);
          } else {
            auto str = StringUtil::Format("%llx", out);
            string_t strAsStr = string_t(strdup(str.c_str()), str.size());
            return StringVector::AddString(result, strAsStr);
          }
        }
      });
}

static void AreNeighborCellsFunction(DataChunk &args, ExpressionState &state,
                                     Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<uint64_t, uint64_t, bool>(
      inputs, inputs2, result, args.size(),
      [&](uint64_t input, uint64_t input2, ValidityMask &mask, idx_t idx) {
        int out;
        H3Error err = areNeighborCells(input, input2, &out);
        if (err) {
          mask.SetInvalid(idx);
          return bool(false);
        } else {
          return bool(out);
        }
      });
}

static void AreNeighborCellsVarcharFunction(DataChunk &args,
                                            ExpressionState &state,
                                            Vector &result) {
  auto &inputs = args.data[0];
  auto &inputs2 = args.data[1];
  BinaryExecutor::ExecuteWithNulls<string_t, string_t, bool>(
      inputs, inputs2, result, args.size(),
      [&](string_t inputStr, string_t inputStr2, ValidityMask &mask,
          idx_t idx) {
        H3Index input, input2;
        H3Error err0 = stringToH3(inputStr.GetString().c_str(), &input);
        H3Error err1 = stringToH3(inputStr2.GetString().c_str(), &input2);
        if (err0 || err1) {
          mask.SetInvalid(idx);
          return bool(false);
        } else {
          int out;
          H3Error err2 = areNeighborCells(input, input2, &out);
          if (err2) {
            mask.SetInvalid(idx);
            return bool(false);
          } else {
            return bool(out);
          }
        }
      });
}

static void IsValidDirectedEdgeVarcharFunction(DataChunk &args,
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
        return bool(isValidDirectedEdge(h));
      });
}

static void IsValidDirectedEdgeFunction(DataChunk &args, ExpressionState &state,
                                        Vector &result) {
  auto &inputs = args.data[0];
  UnaryExecutor::Execute<H3Index, bool>(
      inputs, result, args.size(),
      [&](H3Index input) { return bool(isValidDirectedEdge(input)); });
}

struct DirectedEdgeToBoundaryOperator {
  Vector &result;
  DirectedEdgeToBoundaryOperator(Vector &_result) : result(_result) {}

  string_t operator()(uint64_t input, ValidityMask &mask, idx_t idx) {
    CellBoundary boundary;
    H3Error err = directedEdgeToBoundary(input, &boundary);

    if (err) {
      mask.SetInvalid(idx);
      return StringVector::EmptyString(result, 0);
    } else {
      std::string str = "LINESTRING ((";
      for (int i = 0; i <= boundary.numVerts; i++) {
        std::string sep = (i == 0) ? "" : ", ";
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

static void DirectedEdgeToBoundaryWktFunction(DataChunk &args,
                                              ExpressionState &state,
                                              Vector &result) {
  UnaryExecutor::ExecuteWithNulls<uint64_t, string_t,
                                  DirectedEdgeToBoundaryOperator>(
      args.data[0], result, args.size(),
      DirectedEdgeToBoundaryOperator{result});
}

struct DirectedEdgeToBoundaryVarcharOperator {
  Vector &result;
  DirectedEdgeToBoundaryVarcharOperator(Vector &_result) : result(_result) {}

  string_t operator()(string_t input, ValidityMask &mask, idx_t idx) {
    H3Index h;
    H3Error err = stringToH3(input.GetString().c_str(), &h);
    if (err) {
      mask.SetInvalid(idx);
      return StringVector::EmptyString(result, 0);
    } else {
      return DirectedEdgeToBoundaryOperator(result)(h, mask, idx);
    }
  }
};

static void DirectedEdgeToBoundaryWktVarcharFunction(DataChunk &args,
                                                     ExpressionState &state,
                                                     Vector &result) {
  UnaryExecutor::ExecuteWithNulls<string_t, string_t>(
      args.data[0], result, args.size(),
      DirectedEdgeToBoundaryVarcharOperator{result});
}

CreateScalarFunctionInfo H3Functions::GetDirectedEdgeToCellsFunction() {
  ScalarFunctionSet funcs("h3_directed_edge_to_cells");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
                                   LogicalType::LIST(LogicalType::UBIGINT),
                                   DirectedEdgeToCellsFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
                                   LogicalType::LIST(LogicalType::UBIGINT),
                                   DirectedEdgeToCellsFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                   LogicalType::LIST(LogicalType::VARCHAR),
                                   DirectedEdgeToCellsVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetOriginToDirectedEdgesFunction() {
  ScalarFunctionSet funcs("h3_origin_to_directed_edges");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
                                   LogicalType::LIST(LogicalType::UBIGINT),
                                   OriginToDirectedEdgesFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
                                   LogicalType::LIST(LogicalType::UBIGINT),
                                   OriginToDirectedEdgesFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR},
                                   LogicalType::LIST(LogicalType::VARCHAR),
                                   OriginToDirectedEdgesVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetDirectedEdgeOriginFunction() {
  ScalarFunctionSet funcs("h3_get_directed_edge_origin");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::UBIGINT,
                                   GetDirectedEdgeOriginFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BIGINT,
                                   GetDirectedEdgeOriginFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                   GetDirectedEdgeOriginVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetDirectedEdgeDestinationFunction() {
  ScalarFunctionSet funcs("h3_get_directed_edge_destination");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::UBIGINT,
                                   GetDirectedEdgeDestinationFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BIGINT,
                                   GetDirectedEdgeDestinationFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                   GetDirectedEdgeDestinationVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetCellsToDirectedEdgeFunction() {
  ScalarFunctionSet funcs("h3_cells_to_directed_edge");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::UBIGINT},
                                   LogicalType::UBIGINT,
                                   CellsToDirectedEdgeFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT},
                                   LogicalType::BIGINT,
                                   CellsToDirectedEdgeFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   LogicalType::VARCHAR,
                                   CellsToDirectedEdgeVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetAreNeighborCellsFunction() {
  ScalarFunctionSet funcs("h3_are_neighbor_cells");
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::UBIGINT},
                                   LogicalType::BOOLEAN,
                                   AreNeighborCellsFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT},
                                   LogicalType::BOOLEAN,
                                   AreNeighborCellsFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR},
                                   LogicalType::BOOLEAN,
                                   AreNeighborCellsVarcharFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetIsValidDirectedEdgeFunctions() {
  ScalarFunctionSet funcs("h3_is_valid_directed_edge");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN,
                                   IsValidDirectedEdgeVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::BOOLEAN,
                                   IsValidDirectedEdgeFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BOOLEAN,
                                   IsValidDirectedEdgeFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetDirectedEdgeToBoundaryWktFunction() {
  ScalarFunctionSet funcs("h3_directed_edge_to_boundary_wkt");
  funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::VARCHAR,
                                   DirectedEdgeToBoundaryWktVarcharFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::VARCHAR,
                                   DirectedEdgeToBoundaryWktFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::VARCHAR,
                                   DirectedEdgeToBoundaryWktFunction));
  return CreateScalarFunctionInfo(funcs);
}

} // namespace duckdb
