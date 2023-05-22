#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void GridDiskFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];

	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t origin = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
		int32_t k = args.GetValue(1, i).DefaultCastAs(LogicalType::INTEGER).GetValue<int32_t>();
		int64_t sz;
		int64_t actual = 0;
		H3Error err1 = maxGridDiskSize(k, &sz);
		ThrowH3Error(err1);
		H3Index *out = new H3Index[sz];
		H3Error err2 = gridDisk(origin, k, out);
		ThrowH3Error(err2);
		for (int64_t j = 0; j < sz; j++) {
			auto val = out[j];
			if (val != H3_NULL) {
				ListVector::PushBack(result, Value::UBIGINT(val));
				actual++;
			}
		}

		result_data[i].length = actual;
	}
	result.Verify(args.size());
}

CreateScalarFunctionInfo H3Functions::GetGridDiskFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_grid_disk", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::LIST(LogicalType::UBIGINT), GridDiskFunction));
}

} // namespace duckdb
