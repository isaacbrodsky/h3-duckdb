#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void LatLngToCellFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	auto &inputs3 = args.data[2];
	TernaryExecutor::Execute<double, double, int, H3Index>(inputs, inputs2, inputs3, result, args.size(), [&](double lat, double lng, int res) {
		H3Index cell;
		LatLng latLng = {.lat = lat, .lng= lng};
		H3Error err = latLngToCell(&latLng, res, &cell);
		ThrowH3Error(err);
		return cell;
	});
}

CreateScalarFunctionInfo H3Functions::GetLatLngToCellFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_latlng_to_cell", {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::INTEGER},
	                                               LogicalType::UBIGINT, LatLngToCellFunction, false, nullptr, nullptr,
	                                               nullptr));
}

} // namespace duckdb
