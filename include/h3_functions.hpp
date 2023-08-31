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
		functions.push_back(GetCellToLatLngFunction());
		functions.push_back(GetCellToBoundaryWktFunction());

		// Inspection
		functions.push_back(GetGetResolutionFunction());
		functions.push_back(GetGetBaseCellNumberFunction());
		functions.push_back(GetStringToH3Function());
		functions.push_back(GetH3ToStringFunction());
		functions.push_back(GetIsValidCellFunctions());
		functions.push_back(GetIsResClassIIIFunction());
		functions.push_back(GetIsPentagonFunction());
		functions.push_back(GetGetIcosahedronFacesFunction());

		// Hierarchy
		functions.push_back(GetCellToParentFunction());
		functions.push_back(GetCellToChildrenFunction());
		functions.push_back(GetCellToCenterChildFunction());
		functions.push_back(GetCellToChildPosFunction());
		functions.push_back(GetChildPosToCellFunction());
		functions.push_back(GetCompactCellsFunction());
		functions.push_back(GetUncompactCellsFunction());

		// Traversal
		functions.push_back(GetGridDiskFunction());
		functions.push_back(GetGridDiskDistancesFunction());
		functions.push_back(GetGridDiskUnsafeFunction());
		functions.push_back(GetGridDiskDistancesUnsafeFunction());
		functions.push_back(GetGridDiskDistancesSafeFunction());
		functions.push_back(GetGridRingUnsafeFunction());
		functions.push_back(GetGridPathCellsFunction());
		functions.push_back(GetGridDistanceFunction());
		functions.push_back(GetCellToLocalIjFunction());
		functions.push_back(GetLocalIjToCellFunction());

		// Directed edge
		functions.push_back(GetAreNeighborCellsFunction());
		functions.push_back(GetCellsToDirectedEdgeFunction());
		functions.push_back(GetIsValidDirectedEdgeFunctions());
		functions.push_back(GetGetDirectedEdgeOriginFunction());
		functions.push_back(GetGetDirectedEdgeDestinationFunction());
		functions.push_back(GetDirectedEdgeToCellsFunction());
		functions.push_back(GetOriginToDirectedEdgesFunction());
		functions.push_back(GetDirectedEdgeToBoundaryWktFunction());

		// Vertex
		functions.push_back(GetCellToVertexFunction());
		functions.push_back(GetCellToVertexesFunction());
		functions.push_back(GetVertexToLatFunction());
		functions.push_back(GetVertexToLngFunction());
		functions.push_back(GetVertexToLatLngFunction());
		functions.push_back(GetIsValidVertexFunctions());

		// Misc
		functions.push_back(GetGetHexagonAreaAvgFunction());
		functions.push_back(GetCellAreaFunction());
		functions.push_back(GetGetHexagonEdgeLengthAvgFunction());
		functions.push_back(GetEdgeLengthFunction());
		functions.push_back(GetGetNumCellsFunction());
		functions.push_back(GetGetRes0CellsFunction());
		functions.push_back(GetGetPentagonsFunction());
		functions.push_back(GetGreatCircleDistanceFunction());

		return functions;
	}

private:
	// Indexing
	static CreateScalarFunctionInfo GetLatLngToCellFunction();
	static CreateScalarFunctionInfo GetCellToLatFunction();
	static CreateScalarFunctionInfo GetCellToLngFunction();
	static CreateScalarFunctionInfo GetCellToLatLngFunction();
	static CreateScalarFunctionInfo GetCellToBoundaryWktFunction();

	// Inspection
	static CreateScalarFunctionInfo GetGetResolutionFunction();
	static CreateScalarFunctionInfo GetGetBaseCellNumberFunction();
	static CreateScalarFunctionInfo GetStringToH3Function();
	static CreateScalarFunctionInfo GetH3ToStringFunction();
	static CreateScalarFunctionInfo GetIsValidCellFunctions();
	static CreateScalarFunctionInfo GetIsResClassIIIFunction();
	static CreateScalarFunctionInfo GetIsPentagonFunction();
	static CreateScalarFunctionInfo GetGetIcosahedronFacesFunction();

	// Hierarchy
	static CreateScalarFunctionInfo GetCellToParentFunction();
	static CreateScalarFunctionInfo GetCellToChildrenFunction();
	static CreateScalarFunctionInfo GetCellToCenterChildFunction();
	static CreateScalarFunctionInfo GetCellToChildPosFunction();
	static CreateScalarFunctionInfo GetChildPosToCellFunction();
	static CreateScalarFunctionInfo GetCompactCellsFunction();
	static CreateScalarFunctionInfo GetUncompactCellsFunction();

	// Traversal
	static CreateScalarFunctionInfo GetGridDiskFunction();
	static CreateScalarFunctionInfo GetGridDiskDistancesFunction();
	static CreateScalarFunctionInfo GetGridDiskUnsafeFunction();
	static CreateScalarFunctionInfo GetGridDiskDistancesUnsafeFunction();
	static CreateScalarFunctionInfo GetGridDiskDistancesSafeFunction();
	static CreateScalarFunctionInfo GetGridRingUnsafeFunction();
	static CreateScalarFunctionInfo GetGridPathCellsFunction();
	static CreateScalarFunctionInfo GetGridDistanceFunction();
	static CreateScalarFunctionInfo GetCellToLocalIjFunction();
	static CreateScalarFunctionInfo GetLocalIjToCellFunction();

	// Directed edge
	static CreateScalarFunctionInfo GetAreNeighborCellsFunction();
	static CreateScalarFunctionInfo GetCellsToDirectedEdgeFunction();
	static CreateScalarFunctionInfo GetIsValidDirectedEdgeFunctions();
	static CreateScalarFunctionInfo GetGetDirectedEdgeOriginFunction();
	static CreateScalarFunctionInfo GetGetDirectedEdgeDestinationFunction();
	static CreateScalarFunctionInfo GetDirectedEdgeToCellsFunction();
	static CreateScalarFunctionInfo GetOriginToDirectedEdgesFunction();
	static CreateScalarFunctionInfo GetDirectedEdgeToBoundaryWktFunction();

	// Vertex
	static CreateScalarFunctionInfo GetCellToVertexFunction();
	static CreateScalarFunctionInfo GetCellToVertexesFunction();
	static CreateScalarFunctionInfo GetVertexToLatFunction();
	static CreateScalarFunctionInfo GetVertexToLngFunction();
	static CreateScalarFunctionInfo GetVertexToLatLngFunction();
	static CreateScalarFunctionInfo GetIsValidVertexFunctions();

	// Misc
	static CreateScalarFunctionInfo GetGetHexagonAreaAvgFunction();
	static CreateScalarFunctionInfo GetCellAreaFunction();
	static CreateScalarFunctionInfo GetGetHexagonEdgeLengthAvgFunction();
	static CreateScalarFunctionInfo GetEdgeLengthFunction();
	static CreateScalarFunctionInfo GetGetNumCellsFunction();
	static CreateScalarFunctionInfo GetGetRes0CellsFunction();
	static CreateScalarFunctionInfo GetGetPentagonsFunction();
	static CreateScalarFunctionInfo GetGreatCircleDistanceFunction();

	static void AddAliases(vector<string> names, CreateScalarFunctionInfo fun,
	                       vector<CreateScalarFunctionInfo> &functions) {
		for (auto &name : names) {
			fun.name = name;
			functions.push_back(fun);
		}
	}
};

} // namespace duckdb
