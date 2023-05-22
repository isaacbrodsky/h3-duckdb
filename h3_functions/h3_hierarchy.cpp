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

static void CellToChildrenFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t parent = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
		int32_t res = args.GetValue(1, i).DefaultCastAs(LogicalType::INTEGER).GetValue<int32_t>();
		int64_t sz;
		int64_t actual = 0;
		H3Error err1 = cellToChildrenSize(parent, res, &sz);
		ThrowH3Error(err1);
		std::vector<H3Index> out(sz);
		H3Error err2 = cellToChildren(parent, res, out.data());
		ThrowH3Error(err2);
		for (auto val : out) {
			if (val != H3_NULL) {
				ListVector::PushBack(result, Value::UBIGINT(val));
				actual++;
			}
		}

		result_data[i].length = actual;
	}
	result.Verify(args.size());
}

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

CreateScalarFunctionInfo H3Functions::GetCellToChildrenFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_cell_to_children", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::LIST(LogicalType::UBIGINT), CellToChildrenFunction));
}

CreateScalarFunctionInfo H3Functions::GetCellToCenterChildFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_cell_to_center_child",
	                                               {LogicalType::UBIGINT, LogicalType::INTEGER}, LogicalType::UBIGINT,
	                                               CellToCenterChildFunction));
}

} // namespace duckdb
