#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void CellToParentFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::Execute<uint64_t, int, H3Index>(inputs, inputs2, result, args.size(), [&](uint64_t input, int res) {
		H3Index parent;
		H3Error err = cellToParent(input, res, &parent);
		ThrowH3Error(err);
		return parent;
	});
}

// TODO: children, reference regexp_split_to_array

static void CellToCenterChildFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::Execute<uint64_t, int, H3Index>(inputs, inputs2, result, args.size(), [&](uint64_t input, int res) {
		H3Index child;
		H3Error err = cellToCenterChild(input, res, &child);
		ThrowH3Error(err);
		return child;
	});
}

// TODO: compact
// TODO: uncompact

CreateScalarFunctionInfo H3Functions::GetCellToParentFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_cell_to_parent", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::UBIGINT, CellToParentFunction));
}

CreateScalarFunctionInfo H3Functions::GetCellToCenterChildFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_cell_to_center_child",
	                                               {LogicalType::UBIGINT, LogicalType::INTEGER}, LogicalType::UBIGINT,
	                                               CellToCenterChildFunction));
}

} // namespace duckdb
