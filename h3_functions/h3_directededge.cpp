#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void DirectedEdgeToCellsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t edge = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
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

static void OriginToDirectedEdgesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t origin = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
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

static void GetDirectedEdgeOriginFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::ExecuteWithNulls<uint64_t, uint64_t>(inputs, result, args.size(),
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

static void GetDirectedEdgeDestinationFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::ExecuteWithNulls<uint64_t, uint64_t>(inputs, result, args.size(),
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

static void CellsToDirectedEdgeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::ExecuteWithNulls<uint64_t, uint64_t, uint64_t>(
	    inputs, inputs2, result, args.size(), [&](uint64_t input, uint64_t input2, ValidityMask &mask, idx_t idx) {
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

static void AreNeighborCellsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::ExecuteWithNulls<uint64_t, uint64_t, bool>(
	    inputs, inputs2, result, args.size(), [&](uint64_t input, uint64_t input2, ValidityMask &mask, idx_t idx) {
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

static void IsValidDirectedEdgeVarcharFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(inputs, result, args.size(), [&](string_t input) {
		H3Index h;
		H3Error err = stringToH3(input.GetString().c_str(), &h);
		if (err) {
			return false;
		}
		return bool(isValidDirectedEdge(h));
	});
}

static void IsValidDirectedEdgeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::Execute<H3Index, bool>(inputs, result, args.size(),
	                                      [&](H3Index input) { return bool(isValidDirectedEdge(input)); });
}

// TODO: GetDirectedEdgeToBoundary

CreateScalarFunctionInfo H3Functions::GetDirectedEdgeToCellsFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_directed_edge_to_cells", {LogicalType::UBIGINT},
	                                               LogicalType::LIST(LogicalType::UBIGINT),
	                                               DirectedEdgeToCellsFunction));
}

CreateScalarFunctionInfo H3Functions::GetOriginToDirectedEdgesFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_origin_to_directed_edges", {LogicalType::UBIGINT},
	                                               LogicalType::LIST(LogicalType::UBIGINT),
	                                               OriginToDirectedEdgesFunction));
}

CreateScalarFunctionInfo H3Functions::GetGetDirectedEdgeOriginFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_get_directed_edge_origin", {LogicalType::UBIGINT},
	                                               LogicalType::UBIGINT, GetDirectedEdgeOriginFunction));
}

CreateScalarFunctionInfo H3Functions::GetGetDirectedEdgeDestinationFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_get_directed_edge_destination", {LogicalType::UBIGINT},
	                                               LogicalType::UBIGINT, GetDirectedEdgeDestinationFunction));
}

CreateScalarFunctionInfo H3Functions::GetCellsToDirectedEdgeFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_cells_to_directed_edge",
	                                               {LogicalType::UBIGINT, LogicalType::UBIGINT}, LogicalType::UBIGINT,
	                                               CellsToDirectedEdgeFunction));
}

CreateScalarFunctionInfo H3Functions::GetAreNeighborCellsFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_are_neighbor_cells",
	                                               {LogicalType::UBIGINT, LogicalType::UBIGINT}, LogicalType::BOOLEAN,
	                                               AreNeighborCellsFunction));
}

CreateScalarFunctionInfo H3Functions::GetIsValidDirectedEdgeFunctions() {
	ScalarFunctionSet funcs("h3_is_valid_directed_edge");
	funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN, IsValidDirectedEdgeVarcharFunction));
	funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::BOOLEAN, IsValidDirectedEdgeFunction));
	return CreateScalarFunctionInfo(funcs);
}

} // namespace duckdb
