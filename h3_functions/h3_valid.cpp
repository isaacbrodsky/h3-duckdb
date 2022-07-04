#include "h3_common.hpp"
#include "h3_functions.hpp"

namespace duckdb {

static void ValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &inputs = args.data[0];
	UnaryExecutor::Execute<string_t, bool>(
	    inputs, result, args.size(), [&](string_t input) { 
			H3Index h;
			H3Error err = stringToH3(input.GetString().c_str(), &h);
			if (err) {
				return false;
			}
			return bool(isValidCell(h));
		 });
}

CreateScalarFunctionInfo H3Functions::GetValidFunction() {
	return CreateScalarFunctionInfo(ScalarFunction("h3_is_valid_cell", {LogicalType::VARCHAR}, LogicalType::BOOLEAN,
	                                               ValidFunction, false, nullptr, nullptr, nullptr));
}

} // namespace duckdb
