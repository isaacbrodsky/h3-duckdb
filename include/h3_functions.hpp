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

		// Indexing
		functions.push_back(GetLatLngToCellFunction());
		functions.push_back(GetCellToLatFunction());
		functions.push_back(GetCellToLngFunction());

		// Inspection
		functions.push_back(GetGetResolutionFunction());
		functions.push_back(GetGetBaseCellNumberFunction());
		functions.push_back(GetStringToH3Function());
		functions.push_back(GetH3ToStringFunction());
		functions.push_back(GetIsValidCellFunction());
		functions.push_back(GetIsResClassIIIFunction());
		functions.push_back(GetIsPentagonFunction());

		// Hierarchy
		functions.push_back(GetCellToParentFunction());
		functions.push_back(GetCellToChildrenFunction());
		functions.push_back(GetCellToCenterChildFunction());

		// Traversal
		functions.push_back(GetGridDiskFunction());

		return functions;
	}

private:
	// Indexing
	static CreateScalarFunctionInfo GetLatLngToCellFunction();
	static CreateScalarFunctionInfo GetCellToLatFunction();
	static CreateScalarFunctionInfo GetCellToLngFunction();

	// Inspection
	static CreateScalarFunctionInfo GetGetResolutionFunction();
	static CreateScalarFunctionInfo GetGetBaseCellNumberFunction();
	static CreateScalarFunctionInfo GetStringToH3Function();
	static CreateScalarFunctionInfo GetH3ToStringFunction();
	static CreateScalarFunctionInfo GetIsValidCellFunction();
	static CreateScalarFunctionInfo GetIsResClassIIIFunction();
	static CreateScalarFunctionInfo GetIsPentagonFunction();

	// Hierarchy
	static CreateScalarFunctionInfo GetCellToParentFunction();
	static CreateScalarFunctionInfo GetCellToChildrenFunction();
	static CreateScalarFunctionInfo GetCellToCenterChildFunction();

	// Traversal
	static CreateScalarFunctionInfo GetGridDiskFunction();

	static void AddAliases(vector<string> names, CreateScalarFunctionInfo fun,
	                       vector<CreateScalarFunctionInfo> &functions) {
		for (auto &name : names) {
			fun.name = name;
			functions.push_back(fun);
		}
	}
};

} // namespace duckdb
