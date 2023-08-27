#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void GetHexagonAreaAvgFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::ExecuteWithNulls<int, string_t, double>(inputs, inputs2, result, args.size(),
	                                                        [&](int res, string_t unit, ValidityMask &mask, idx_t idx) {
		                                                        double out;
		                                                        H3Error err = E_OPTION_INVALID;
		                                                        if (unit == "km^2") {
			                                                        err = getHexagonAreaAvgKm2(res, &out);
		                                                        } else if (unit == "m^2") {
			                                                        err = getHexagonAreaAvgM2(res, &out);
		                                                        }
		                                                        if (err) {
			                                                        mask.SetInvalid(idx);
			                                                        return 0.0;
		                                                        } else {
			                                                        return out;
		                                                        }
	                                                        });
}

static double CellAreaFunctionInternal(H3Index cell, string_t unit, ValidityMask &mask, idx_t idx) {
	double out;
	H3Error err = E_OPTION_INVALID;
	if (unit == "rads^2") {
		err = cellAreaRads2(cell, &out);
	} else if (unit == "km^2") {
		err = cellAreaKm2(cell, &out);
	} else if (unit == "m^2") {
		err = cellAreaM2(cell, &out);
	}
	if (err) {
		mask.SetInvalid(idx);
		return 0.0;
	} else {
		return out;
	}
}

static void CellAreaVarcharFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::ExecuteWithNulls<string_t, string_t, double>(
	    inputs, inputs2, result, args.size(), [&](string_t cell, string_t unit, ValidityMask &mask, idx_t idx) {
		    H3Index h;
		    H3Error err = stringToH3(cell.GetString().c_str(), &h);
		    if (err) {
			    mask.SetInvalid(idx);
			    return 0.0;
		    }
		    return CellAreaFunctionInternal(h, unit, mask, idx);
	    });
}

static void CellAreaFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::ExecuteWithNulls<H3Index, string_t, double>(inputs, inputs2, result, args.size(),
	                                                            CellAreaFunctionInternal);
}

static void GetHexagonEdgeLengthAvgFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::ExecuteWithNulls<int, string_t, double>(inputs, inputs2, result, args.size(),
	                                                        [&](int res, string_t unit, ValidityMask &mask, idx_t idx) {
		                                                        double out;
		                                                        H3Error err = E_OPTION_INVALID;
		                                                        if (unit == "km") {
			                                                        err = getHexagonEdgeLengthAvgKm(res, &out);
		                                                        } else if (unit == "m") {
			                                                        err = getHexagonEdgeLengthAvgM(res, &out);
		                                                        }
		                                                        if (err) {
			                                                        mask.SetInvalid(idx);
			                                                        return 0.0;
		                                                        } else {
			                                                        return out;
		                                                        }
	                                                        });
}

static double EdgeLengthFunctionInternal(H3Index edge, string_t unit, ValidityMask &mask, idx_t idx) {
	double out;
	H3Error err = E_OPTION_INVALID;
	if (unit == "rads") {
		err = edgeLengthRads(edge, &out);
	} else if (unit == "km") {
		err = edgeLengthKm(edge, &out);
	} else if (unit == "m") {
		err = edgeLengthM(edge, &out);
	}
	if (err) {
		mask.SetInvalid(idx);
		return 0.0;
	} else {
		return out;
	}
}

static void EdgeLengthVarcharFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::ExecuteWithNulls<string_t, string_t, double>(
	    inputs, inputs2, result, args.size(), [&](string_t edge, string_t unit, ValidityMask &mask, idx_t idx) {
		    H3Index h;
		    H3Error err = stringToH3(edge.GetString().c_str(), &h);
		    if (err) {
			    mask.SetInvalid(idx);
			    return 0.0;
		    }
		    return EdgeLengthFunctionInternal(h, unit, mask, idx);
	    });
}

static void EdgeLengthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	auto &inputs2 = args.data[1];
	BinaryExecutor::ExecuteWithNulls<H3Index, string_t, double>(inputs, inputs2, result, args.size(),
	                                                            EdgeLengthFunctionInternal);
}

static void GetNumCellsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::ExecuteWithNulls<int, int64_t>(inputs, result, args.size(),
	                                              [&](int res, ValidityMask &mask, idx_t idx) {
		                                              int64_t out;
		                                              H3Error err = getNumCells(res, &out);
		                                              if (err) {
			                                              mask.SetInvalid(idx);
			                                              return int64_t(0);
		                                              }
		                                              return out;
	                                              });
}

CreateScalarFunctionInfo H3Functions::GetGetHexagonAreaAvgFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_get_hexagon_area_avg",
	                                               {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                                               GetHexagonAreaAvgFunction));
}

CreateScalarFunctionInfo H3Functions::GetCellAreaFunction() {
	ScalarFunctionSet funcs("h3_cell_area");
	funcs.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, CellAreaVarcharFunction));
	funcs.AddFunction(
	    ScalarFunction({LogicalType::UBIGINT, LogicalType::VARCHAR}, LogicalType::DOUBLE, CellAreaFunction));
	return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetHexagonEdgeLengthAvgFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_get_hexagon_edge_length_avg",
	                                               {LogicalType::INTEGER, LogicalType::VARCHAR}, LogicalType::DOUBLE,
	                                               GetHexagonEdgeLengthAvgFunction));
}

CreateScalarFunctionInfo H3Functions::GetEdgeLengthFunction() {
	ScalarFunctionSet funcs("h3_edge_length");
	funcs.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::DOUBLE, EdgeLengthVarcharFunction));
	funcs.AddFunction(
	    ScalarFunction({LogicalType::UBIGINT, LogicalType::VARCHAR}, LogicalType::DOUBLE, EdgeLengthFunction));
	return CreateScalarFunctionInfo(funcs);
}

CreateScalarFunctionInfo H3Functions::GetGetNumCellsFunction() {
	return CreateScalarFunctionInfo(
	    ScalarFunction("h3_get_num_cells", {LogicalType::INTEGER}, LogicalType::BIGINT, GetNumCellsFunction));
}

} // namespace duckdb
