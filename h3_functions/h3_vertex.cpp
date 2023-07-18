#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void CellToVertexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::ExecuteWithNulls<H3Index, int32_t, H3Index>(
	    inputs, inputs2, result, args.size(), [&](H3Index cell, int32_t vertexNum, ValidityMask &mask, idx_t idx) {
		    H3Index vertex;
		    H3Error err = cellToVertex(cell, vertexNum, &vertex);
		    if (err) {
			    mask.SetInvalid(idx);
			    return H3Index(H3_NULL);
		    } else {
			    return vertex;
		    }
	    });
}

static void CellToVertexesFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t cell = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();

		int64_t actual = 0;
		std::vector<H3Index> out(6);
		H3Error err = cellToVertexes(cell, out.data());
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

static void VertexToLatFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::ExecuteWithNulls<H3Index, double>(inputs, result, args.size(),
	                                                 [&](H3Index vertex, ValidityMask &mask, idx_t idx) {
		                                                 LatLng latLng = {.lat = 0, .lng = 0};
		                                                 H3Error err = vertexToLatLng(vertex, &latLng);
		                                                 if (err) {
			                                                 mask.SetInvalid(idx);
			                                                 return .0;
		                                                 } else {
			                                                 return radsToDegs(latLng.lat);
		                                                 }
	                                                 });
}

static void VertexToLngFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::ExecuteWithNulls<H3Index, double>(inputs, result, args.size(),
	                                                 [&](H3Index vertex, ValidityMask &mask, idx_t idx) {
		                                                 LatLng latLng = {.lat = 0, .lng = 0};
		                                                 H3Error err = vertexToLatLng(vertex, &latLng);
		                                                 if (err) {
			                                                 mask.SetInvalid(idx);
			                                                 return .0;
		                                                 } else {
			                                                 return radsToDegs(latLng.lng);
		                                                 }
	                                                 });
}

static void VertexToLatLngFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto result_data = FlatVector::GetData<list_entry_t>(result);
	for (idx_t i = 0; i < args.size(); i++) {
		result_data[i].offset = ListVector::GetListSize(result);

		uint64_t vertex = args.GetValue(0, i).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
		LatLng latLng;
		H3Error err = vertexToLatLng(vertex, &latLng);
		ThrowH3Error(err);

		ListVector::PushBack(result, radsToDegs(latLng.lat));
		ListVector::PushBack(result, radsToDegs(latLng.lng));
		result_data[i].length = 2;
	}
	result.Verify(args.size());
}

static void IsValidVertexVarcharFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(inputs, result, args.size(), [&](string_t input) {
		H3Index h;
		H3Error err = stringToH3(input.GetString().c_str(), &h);
		if (err) {
			return false;
		}
		return bool(isValidVertex(h));
	});
}

static void IsValidVertexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::Execute<H3Index, bool>(inputs, result, args.size(),
	                                      [&](H3Index input) { return bool(isValidVertex(input)); });
}

CreateScalarFunctionInfo H3Functions::GetCellToVertexFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_cell_to_vertex", {LogicalType::UBIGINT, LogicalType::INTEGER},
	                                               LogicalType::UBIGINT, CellToVertexFunction));
}

CreateScalarFunctionInfo H3Functions::GetCellToVertexesFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_cell_to_vertexes", {LogicalType::UBIGINT},
	                                               LogicalType::LIST(LogicalType::UBIGINT), CellToVertexesFunction));
}

CreateScalarFunctionInfo H3Functions::GetVertexToLatFunction() {
	return CreateScalarFunctionInfo(
	    ScalarFunction("h3_vertex_to_lat", {LogicalType::UBIGINT}, LogicalType::DOUBLE, VertexToLatFunction));
}

CreateScalarFunctionInfo H3Functions::GetVertexToLngFunction() {
	return CreateScalarFunctionInfo(
	    ScalarFunction("h3_vertex_to_lng", {LogicalType::UBIGINT}, LogicalType::DOUBLE, VertexToLngFunction));
}

CreateScalarFunctionInfo H3Functions::GetVertexToLatLngFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_vertex_to_latlng", {LogicalType::UBIGINT},
	                                               LogicalType::LIST(LogicalType::DOUBLE), VertexToLatLngFunction));
}

CreateScalarFunctionInfo H3Functions::GetIsValidVertexFunctions() {
	ScalarFunctionSet funcs("h3_is_valid_vertex");
	funcs.AddFunction(ScalarFunction({LogicalType::VARCHAR}, LogicalType::BOOLEAN, IsValidVertexVarcharFunction));
	funcs.AddFunction(ScalarFunction({LogicalType::UBIGINT}, LogicalType::BOOLEAN, IsValidVertexFunction));
	return CreateScalarFunctionInfo(funcs);
}

} // namespace duckdb
