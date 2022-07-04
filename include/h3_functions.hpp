//===----------------------------------------------------------------------===//
//                         DuckDB
//
// h3_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

class H3Functions {
public:
	static vector<CreateScalarFunctionInfo> GetFunctions() {
		vector<CreateScalarFunctionInfo> functions;

		functions.push_back(GetValidFunction());
		functions.push_back(GetCellToParentFunction());
		functions.push_back(GetLatLngToCellFunction());

		return functions;
	}

private:
	static CreateScalarFunctionInfo GetValidFunction();
	static CreateScalarFunctionInfo GetCellToParentFunction();
	static CreateScalarFunctionInfo GetLatLngToCellFunction();

	static void AddAliases(vector<string> names, CreateScalarFunctionInfo fun,
	                       vector<CreateScalarFunctionInfo> &functions) {
		for (auto &name : names) {
			fun.name = name;
			functions.push_back(fun);
		}
	}
};

} // namespace duckdb
