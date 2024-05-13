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
  template <class INPUT_TYPE, class RESULT_TYPE>
  static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
    CellBoundary boundary;
    H3Error err = directedEdgeToBoundary(input, &boundary);

    if (err) {
      // TODO: Is it possible to return null here instead?
      return StringVector::EmptyString(result, 0);
    } else {
      std::string str = "POLYGON ((";
      for (int i = 0; i <= boundary.numVerts; i++) {
        std::string sep = (i == 0) ? "" : ", ";
        int vertIndex = (i == boundary.numVerts) ? 0 : i;
        str += StringUtil::Format("%s%f %f", sep,
                                  radsToDegs(boundary.verts[vertIndex].lng),
                                  radsToDegs(boundary.verts[vertIndex].lat));
      }
      str += "))";

      string_t strAsStr = string_t(strdup(str.c_str()), str.size());
      return StringVector::AddString(result, strAsStr);
    }
  }
};

static void DirectedEdgeToBoundaryWktFunction(DataChunk &args,
                                              ExpressionState &state,
                                              Vector &result) {
  UnaryExecutor::ExecuteString<uint64_t, string_t,
                               DirectedEdgeToBoundaryOperator>(
      args.data[0], result, args.size());
}

struct DirectedEdgeToBoundaryVarcharOperator {
  template <class INPUT_TYPE, class RESULT_TYPE>
  static RESULT_TYPE Operation(INPUT_TYPE input, Vector &result) {
    H3Index h;
    H3Error err = stringToH3(input.GetString().c_str(), &h);
    if (err) {
      return StringVector::EmptyString(result, 0);
    } else {
      return DirectedEdgeToBoundaryOperator().Operation<H3Index, RESULT_TYPE>(
          h, result);
    }
  }
};

static void DirectedEdgeToBoundaryWktVarcharFunction(DataChunk &args,
                                                     ExpressionState &state,
                                                     Vector &result) {
  UnaryExecutor::ExecuteString<string_t, string_t,
                               DirectedEdgeToBoundaryVarcharOperator>(
      args.data[0], result, args.size());
}

CreateScalarFunctionInfo H3Functions::GetDirectedEdgeToCellsFunction() {
  ScalarFunctionSet funcs("h3_directed_edge_to_cells");
  // TODO: VARCHAR variant of this function
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
                                   LogicalType::LIST(LogicalType::UBIGINT),
                                   DirectedEdgeToCellsFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
                                   LogicalType::LIST(LogicalType::UBIGINT),
                                   DirectedEdgeToCellsFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetOriginToDirectedEdgesFunction() {
  ScalarFunctionSet funcs("h3_origin_to_directed_edges");
  // TODO: VARCHAR variant of this function
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT},
                                   LogicalType::LIST(LogicalType::UBIGINT),
                                   OriginToDirectedEdgesFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT},
                                   LogicalType::LIST(LogicalType::UBIGINT),
                                   OriginToDirectedEdgesFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetDirectedEdgeOriginFunction() {
  ScalarFunctionSet funcs("h3_get_directed_edge_origin");
  // TODO: VARCHAR variant of this function
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::UBIGINT,
                                   GetDirectedEdgeOriginFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BIGINT,
                                   GetDirectedEdgeOriginFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetDirectedEdgeDestinationFunction() {
  ScalarFunctionSet funcs("h3_get_directed_edge_destination");
  // TODO: VARCHAR variant of this function
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::UBIGINT,
                                   GetDirectedEdgeDestinationFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT}, LogicalType::BIGINT,
                                   GetDirectedEdgeDestinationFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetCellsToDirectedEdgeFunction() {
  ScalarFunctionSet funcs("h3_cells_to_directed_edge");
  // TODO: VARCHAR variant of this function
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::UBIGINT},
                                   LogicalType::UBIGINT,
                                   CellsToDirectedEdgeFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT},
                                   LogicalType::BIGINT,
                                   CellsToDirectedEdgeFunction));
  return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetAreNeighborCellsFunction() {
  ScalarFunctionSet funcs("h3_are_neighbor_cells");
  // TODO: VARCHAR variant of this function
  funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT, LogicalType::UBIGINT},
                                   LogicalType::BOOLEAN,
                                   AreNeighborCellsFunction));
  funcs.AddFunction(ScalarFunction({LogicalType::BIGINT, LogicalType::BIGINT},
                                   LogicalType::BOOLEAN,
                                   AreNeighborCellsFunction));
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
