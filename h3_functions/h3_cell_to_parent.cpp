#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void CellToParentFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::Execute<uint64_t, int, H3Index>(
	    inputs, inputs2, result, args.size(), [&](uint64_t input, int res) {
			H3Index parent;
			H3Error err = cellToParent(input, res, &parent);
			ThrowH3Error(err);
			return parent;
		 });
}

CreateScalarFunctionInfo H3Functions::GetCellToParentFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_cell_to_parent", {LogicalType::UBIGINT, LogicalType::INTEGER}, LogicalType::UBIGINT,
	                                               CellToParentFunction, false, nullptr, nullptr, nullptr));
}

} // namespace duckdb
