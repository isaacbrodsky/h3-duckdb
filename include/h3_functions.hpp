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

		functions.push_back(GetLatLngToCellFunction());
		functions.push_back(GetCellToLatFunction());
		functions.push_back(GetCellToLngFunction());
		functions.push_back(GetValidFunction());
		functions.push_back(GetCellToParentFunction());

		return functions;
	}

private:
	static CreateScalarFunctionInfo GetLatLngToCellFunction();
	static CreateScalarFunctionInfo GetCellToLatFunction();
	static CreateScalarFunctionInfo GetCellToLngFunction();
	static CreateScalarFunctionInfo GetValidFunction();
	static CreateScalarFunctionInfo GetCellToParentFunction();

	static void AddAliases(vector<string> names, CreateScalarFunctionInfo fun,
	                       vector<CreateScalarFunctionInfo> &functions) {
		for (auto &name : names) {
			fun.name = name;
			functions.push_back(fun);
		}
	}
};

} // namespace duckdb
