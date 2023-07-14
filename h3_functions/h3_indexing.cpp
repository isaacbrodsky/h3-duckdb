#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void LatLngToCellFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	auto &inputs3 = args.data[2];
	TernaryExecutor::ExecuteWithNulls<double, double, int, H3Index>(
	    inputs, inputs2, inputs3, result, args.size(),
	    [&](double lat, double lng, int res, ValidityMask &mask, idx_t idx) {
		    H3Index cell;
		    LatLng latLng = {.lat = degsToRads(lat), .lng = degsToRads(lng)};
		    H3Error err = latLngToCell(&latLng, res, &cell);
		    if (err) {
			    mask.SetInvalid(idx);
			    return H3Index(H3_NULL);
		    } else {
			    return cell;
		    }
	    });
}

static void CellToLatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::ExecuteWithNulls<H3Index, double>(inputs, result, args.size(),
	                                                 [&](H3Index cell, ValidityMask &mask, idx_t idx) {
		                                                 LatLng latLng = {.lat = 0, .lng = 0};
		                                                 H3Error err = cellToLatLng(cell, &latLng);
		                                                 if (err) {
			                                                 mask.SetInvalid(idx);
			                                                 return .0;
		                                                 } else {
			                                                 return radsToDegs(latLng.lat);
		                                                 }
	                                                 });
}

static void CellToLngFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::ExecuteWithNulls<H3Index, double>(inputs, result, args.size(),
	                                                 [&](H3Index cell, ValidityMask &mask, idx_t idx) {
		                                                 LatLng latLng = {.lat = 0, .lng = 0};
		                                                 H3Error err = cellToLatLng(cell, &latLng);
		                                                 if (err) {
			                                                 mask.SetInvalid(idx);
			                                                 return .0;
		                                                 } else {
			                                                 return radsToDegs(latLng.lng);
		                                                 }
	                                                 });
}

static void CellToLatLngFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t cell = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
		LatLng latLng;
		H3Error err = cellToLatLng(cell, &latLng);
		if (err) {
			result.SetValue(i, Value(LogicalType::SQLNULL));
		} else {
			ListVector::PushBack(result, radsToDegs(latLng.lat));
			ListVector::PushBack(result, radsToDegs(latLng.lng));
			result_data[i].length = 2;
		}
	}
	result.Verify(args.size());
}

// TODO: cellToBoundary

CreateScalarFunctionInfo H3Functions::GetLatLngToCellFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_latlng_to_cell",
	                                               {LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::INTEGER},
	                                               LogicalType::UBIGINT, LatLngToCellFunction));
}

CreateScalarFunctionInfo H3Functions::GetCellToLatFunction() {
	return CreateScalarFunctionInfo(
	    ScalarFunction("h3_cell_to_lat", {LogicalType::UBIGINT}, LogicalType::DOUBLE, CellToLatFunction));
}

CreateScalarFunctionInfo H3Functions::GetCellToLngFunction() {
	return CreateScalarFunctionInfo(
	    ScalarFunction("h3_cell_to_lng", {LogicalType::UBIGINT}, LogicalType::DOUBLE, CellToLngFunction));
}

CreateScalarFunctionInfo H3Functions::GetCellToLatLngFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_cell_to_latlng", {LogicalType::UBIGINT},
	                                               LogicalType::LIST(LogicalType::DOUBLE), CellToLatLngFunction));
}

} // namespace duckdb
