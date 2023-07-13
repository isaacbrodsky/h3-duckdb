#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void GridDiskFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t origin = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
		int32_t k = args.GetValue(1, i).DefaultCastAs(LogicalType::INTEGER).GetValue<int32_t>();
		int64_t sz;
		int64_t actual = 0;
		H3Error err1 = maxGridDiskSize(k, &sz);
		ThrowH3Error(err1);
		std::vector<H3Index> out(sz);
		H3Error err2 = gridDisk(origin, k, out.data());
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

static void GridDiskDistancesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
		H3Error err2 = gridDiskDistances(origin, k, out.data(), distancesOut.data());
		ThrowH3Error(err2);

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
	result.Verify(args.size());
}

static void GridDiskUnsafeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t origin = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
		int32_t k = args.GetValue(1, i).DefaultCastAs(LogicalType::INTEGER).GetValue<int32_t>();
		int64_t sz;
		H3Error err1 = maxGridDiskSize(k, &sz);
		ThrowH3Error(err1);
		std::vector<H3Index> out(sz);
		H3Error err2 = gridDiskUnsafe(origin, k, out.data());
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

static void GridDiskDistancesUnsafeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
		H3Error err2 = gridDiskDistancesUnsafe(origin, k, out.data(), distancesOut.data());
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

static void GridDiskDistancesSafeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
		H3Error err2 = gridDiskDistancesSafe(origin, k, out.data(), distancesOut.data());
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

CreateScalarFunctionInfo H3Functions::GetGridDiskFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_grid_disk", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::LIST(LogicalType::UBIGINT), GridDiskFunction));
}

CreateScalarFunctionInfo H3Functions::GetGridDiskDistancesFunction() {
	return CreateScalarFunctionInfo(
	    ScalarFunction("h3_grid_disk_distances", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                   LogicalType::LIST(LogicalType::LIST(LogicalType::UBIGINT)), GridDiskDistancesFunction));
}

CreateScalarFunctionInfo H3Functions::GetGridDiskUnsafeFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_grid_disk_unsafe", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::LIST(LogicalType::UBIGINT), GridDiskUnsafeFunction));
}

CreateScalarFunctionInfo H3Functions::GetGridDiskDistancesUnsafeFunction() {
	return CreateScalarFunctionInfo(
	    ScalarFunction("h3_grid_disk_distances_unsafe", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                   LogicalType::LIST(LogicalType::LIST(LogicalType::UBIGINT)), GridDiskDistancesUnsafeFunction));
}

CreateScalarFunctionInfo H3Functions::GetGridDiskDistancesSafeFunction() {
	return CreateScalarFunctionInfo(
	    ScalarFunction("h3_grid_disk_distances_safe", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                   LogicalType::LIST(LogicalType::LIST(LogicalType::UBIGINT)), GridDiskDistancesSafeFunction));
}

} // namespace duckdb
