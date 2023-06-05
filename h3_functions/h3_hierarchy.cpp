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

static void CellToChildPosFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::Execute<H3Index, int, int64_t>(inputs, inputs2, result, args.size(), [&](H3Index input, int res) {
		int64_t child;
		H3Error err = cellToChildPos(input, res, &child);
		ThrowH3Error(err);
		return child;
	});
}

static void ChildPosToCellFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	auto &inputs3 = args.data[2];
	TernaryExecutor::Execute<int64_t, H3Index, int, H3Index>(inputs, inputs2, inputs3, result, args.size(),
	                                                         [&](int64_t pos, H3Index input, int res) {
		                                                         H3Index child;
		                                                         H3Error err = childPosToCell(pos, input, res, &child);
		                                                         ThrowH3Error(err);
		                                                         return child;
	                                                         });
}

static void CompactCellsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 1);
	auto count = args.size();

	Vector &lhs = args.data[0];
	if (lhs.GetType().id() == LogicalTypeId::SQLNULL) {
		result.Reference(lhs);
		return;
	}

	UnifiedVectorFormat lhs_data;
	lhs.ToUnifiedFormat(count, lhs_data);

	auto list_entries = (list_entry_t *)lhs_data.data;

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	idx_t offset = 0;
	for (idx_t i = 0; i < count; i++) {
		printf("i=%llx\n", i);
		result_entries[i].offset = offset;
		result_entries[i].length = 0;
		auto list_index = lhs_data.sel->get_index(i);

		if (!lhs_data.validity.RowIsValid(list_index)) {
			printf("invalid\n");
			result_validity.SetInvalid(i);
			continue;
		}

		const auto &list_entry = list_entries[list_index];

		const auto lvalue = lhs.GetValue(list_entry.offset).DefaultCastAs(LogicalType::LIST(LogicalType::UBIGINT));
		;

		auto &list_children = ListValue::GetChildren(lvalue);

		int64_t actual = 0;
		for (const auto &child_val : list_children) {
			printf("child_val=%llx\n", child_val.GetValue<uint64_t>());
			ListVector::PushBack(result, child_val);
			actual++;
		}

		// const auto &lhs_entry = lhs_entries[lhs_list_index];
		// result_entries[i].length += lhs_entry.length;
		// //Vector &target, const Vector &source, const SelectionVector &sel, idx_t source_size, idx_t source_offset
		// ListVector::Append(result, lhs_child, *lhs_child_data.sel, lhs_entry.offset + lhs_entry.length,
		//                    lhs_entry.offset);

		printf("list_entry.length=%llx\n", list_entry.length);
		result_entries[i].length = actual;
		offset += list_entry.length; // result_entries[i].length;
	}
	// D_ASSERT(ListVector::GetListSize(result) == offset);

	result.Verify(args.size());

	printf("out\n");
	if (lhs.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void UncompactCellsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	// TODO
}

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

CreateScalarFunctionInfo H3Functions::GetCellToChildPosFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_cell_to_child_pos", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::BIGINT, CellToChildPosFunction));
}

CreateScalarFunctionInfo H3Functions::GetChildPosToCellFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_child_pos_to_cell",
	                                               {LogicalType::BIGINT, LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::UBIGINT, ChildPosToCellFunction));
}

CreateScalarFunctionInfo H3Functions::GetCompactCellsFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_compact_cells", {LogicalType::LIST(LogicalType::UBIGINT)},
	                                               LogicalType::LIST(LogicalType::UBIGINT), CompactCellsFunction));
}

CreateScalarFunctionInfo H3Functions::GetUncompactCellsFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_uncompact_cells",
	                                               {LogicalType::LIST(LogicalType::UBIGINT), LogicalType::INTEGER},
	                                               LogicalType::LIST(LogicalType::UBIGINT), UncompactCellsFunction));
}

} // namespace duckdb
