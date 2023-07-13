#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

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

template <class Fn>
static void GridDiskTmplFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t origin = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
		int32_t k = args.GetValue(1, i).DefaultCastAs(LogicalType::INTEGER).GetValue<int32_t>();
		int64_t sz;
		H3Error err1 = maxGridDiskSize(k, &sz);
		ThrowH3Error(err1);
		std::vector<H3Index> out(sz);
		H3Error err2 = Fn::fn(origin, k, out.data());
		if (err2) {
			result_data[i].length = 0;
		} else {
			int64_t actual = 0;
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

struct GridDiskDistancesOperator {
	static H3Error fn(H3Index origin, int32_t k, H3Index *out, int32_t *distancesOut) {
		return gridDiskDistances(origin, k, out, distancesOut);
	}
};

struct GridDiskDistancesSafeOperator {
	static H3Error fn(H3Index origin, int32_t k, H3Index *out, int32_t *distancesOut) {
		return gridDiskDistancesSafe(origin, k, out, distancesOut);
	}
};

struct GridDiskDistancesUnsafeOperator {
	static H3Error fn(H3Index origin, int32_t k, H3Index *out, int32_t *distancesOut) {
		return gridDiskDistancesUnsafe(origin, k, out, distancesOut);
	}
};

template <class Fn>
static void GridDiskDistancesTmplFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t origin = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
		int32_t k = args.GetValue(1, i).DefaultCastAs(LogicalType::INTEGER).GetValue<int32_t>();
		int64_t sz;
		H3Error err1 = maxGridDiskSize(k, &sz);
		ThrowH3Error(err1);
		std::vector<H3Index> out(sz);
		std::vector<int32_t> distancesOut(sz);
		H3Error err2 = Fn::fn(origin, k, out.data(), distancesOut.data());
		if (err2) {
			result_data[i].length = 0;
		} else {
			// Reorganize the results similar to H3-Java sorted list of list of indexes
			// std vector of duckdb vector
			std::vector<vector<Value>> results(k + 1);
			for (idx_t j = 0; j < out.size(); j++) {
				if (out[j] != H3_NULL) {
					results[distancesOut[j]].push_back(Value::UBIGINT(out[j]));
				}
			}

			int64_t actual = 0;
			for (auto val : results) {
				ListVector::PushBack(result, Value::LIST(LogicalType::UBIGINT, val));
				actual++;
			}

			result_data[i].length = actual;
		}
	}
	result.Verify(args.size());
}
static void GridRingUnsafeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t origin = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
		int32_t k = args.GetValue(1, i).DefaultCastAs(LogicalType::INTEGER).GetValue<int32_t>();
		int64_t sz = k == 0 ? 1 : 6 * k;
		std::vector<H3Index> out(sz);
		H3Error err2 = gridRingUnsafe(origin, k, out.data());
		if (err2) {
			result_data[i].length = 0;
		} else {
			int64_t actual = 0;
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

// TODO: gridDisksUnsafe?

CreateScalarFunctionInfo H3Functions::GetGridDiskFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_grid_disk", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::LIST(LogicalType::UBIGINT),
	                                               GridDiskTmplFunction<GridDiskOperator>));
}

CreateScalarFunctionInfo H3Functions::GetGridDiskDistancesFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_grid_disk_distances",
	                                               {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::LIST(LogicalType::LIST(LogicalType::UBIGINT)),
	                                               GridDiskDistancesTmplFunction<GridDiskDistancesOperator>));
}

CreateScalarFunctionInfo H3Functions::GetGridDiskUnsafeFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_grid_disk_unsafe", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::LIST(LogicalType::UBIGINT),
	                                               GridDiskTmplFunction<GridDiskUnsafeOperator>));
}

CreateScalarFunctionInfo H3Functions::GetGridDiskDistancesUnsafeFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_grid_disk_distances_unsafe",
	                                               {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::LIST(LogicalType::LIST(LogicalType::UBIGINT)),
	                                               GridDiskDistancesTmplFunction<GridDiskDistancesUnsafeOperator>));
}

CreateScalarFunctionInfo H3Functions::GetGridDiskDistancesSafeFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_grid_disk_distances_safe",
	                                               {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::LIST(LogicalType::LIST(LogicalType::UBIGINT)),
	                                               GridDiskDistancesTmplFunction<GridDiskDistancesSafeOperator>));
}

CreateScalarFunctionInfo H3Functions::GetGridRingUnsafeFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_grid_ring_unsafe", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::LIST(LogicalType::UBIGINT), GridRingUnsafeFunction));
}

} // namespace duckdb
