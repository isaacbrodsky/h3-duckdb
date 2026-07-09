//===----------------------------------------------------------------------===//
//                         DuckDB
//
// h3_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "h3_common.hpp"
#include <vector>

namespace h3duckdb {

class H3Functions {
public:
  static std::pair<std::vector<duckdb_scalar_function>,
                   std::vector<duckdb_scalar_function_set>>
  GetFunctions() {
    std::vector<duckdb_scalar_function> functions;
    std::vector<duckdb_scalar_function_set> functionSets;

    // Indexing
    functions.push_back(GetLatLngToCellFunction());
    functions.push_back(GetLatLngToCellVarcharFunction());
    functionSets.push_back(GetCellToLatFunction());
    functionSets.push_back(GetCellToLngFunction());
    //    functions.push_back(GetCellToLatLngFunction());
    //    functions.push_back(GetCellToBoundaryWktFunction());
    //    functions.push_back(GetCellToBoundaryWkbFunction());

    // Inspection
    //    functions.push_back(GetGetResolutionFunction());
    //    functions.push_back(GetGetBaseCellNumberFunction());
    functions.push_back(GetStringToH3Function());
    functionSets.push_back(GetH3ToStringFunction());
    //    functions.push_back(GetIsValidIndexFunctions());
    //    functions.push_back(GetIsValidCellFunctions());
    //    functions.push_back(GetIsResClassIIIFunction());
    //    functions.push_back(GetIsPentagonFunction());
    //    functions.push_back(GetGetIcosahedronFacesFunction());
    //    functions.push_back(GetGetIndexDigitFunction());
    //    functions.push_back(GetConstructCellFunction());
    //    functions.push_back(GetConstructCellVarcharFunction());
    //
    //    // Hierarchy
    //    functions.push_back(GetCellToParentFunction());
    //    functions.push_back(GetCellToChildrenFunction());
    //    functions.push_back(GetCellToChildrenSizeFunction());
    //    functions.push_back(GetCellToCenterChildFunction());
    //    functions.push_back(GetCellToChildPosFunction());
    //    functions.push_back(GetChildPosToCellFunction());
    //    functions.push_back(GetCompactCellsFunction());
    //    functions.push_back(GetUncompactCellsFunction());
    //
    //    // Traversal
    //    functions.push_back(GetGridDiskFunction());
    //    functions.push_back(GetGridDiskDistancesFunction());
    //    functions.push_back(GetGridDiskUnsafeFunction());
    //    functions.push_back(GetGridDiskDistancesUnsafeFunction());
    //    functions.push_back(GetGridDiskDistancesSafeFunction());
    //    functions.push_back(GetGridRingFunction());
    //    functions.push_back(GetGridRingUnsafeFunction());
    //    functions.push_back(GetGridPathCellsFunction());
    //    functions.push_back(GetGridDistanceFunction());
    //    functions.push_back(GetMaxGridDiskSizeFunction());
    //    functions.push_back(GetCellToLocalIjFunction());
    //    functions.push_back(GetLocalIjToCellFunction());
    //
    //    // Directed edge
    //    functions.push_back(GetAreNeighborCellsFunction());
    //    functions.push_back(GetCellsToDirectedEdgeFunction());
    //    functions.push_back(GetIsValidDirectedEdgeFunctions());
    //    functions.push_back(GetGetDirectedEdgeOriginFunction());
    //    functions.push_back(GetGetDirectedEdgeDestinationFunction());
    //    functions.push_back(GetDirectedEdgeToCellsFunction());
    //    functions.push_back(GetOriginToDirectedEdgesFunction());
    //    functions.push_back(GetDirectedEdgeToBoundaryWktFunction());
    //    functions.push_back(GetDirectedEdgeToBoundaryWkbFunction());
    //    functions.push_back(GetReverseDirectedEdgeFunction());
    //
    //    // Vertex
    //    functions.push_back(GetCellToVertexFunction());
    //    functions.push_back(GetCellToVertexesFunction());
    //    functions.push_back(GetVertexToLatFunction());
    //    functions.push_back(GetVertexToLngFunction());
    //    functions.push_back(GetVertexToLatLngFunction());
    //    functions.push_back(GetIsValidVertexFunctions());
    //
    //    // Misc
    //    functions.push_back(GetGetHexagonAreaAvgFunction());
    //    functions.push_back(GetCellAreaFunction());
    //    functions.push_back(GetGetHexagonEdgeLengthAvgFunction());
    //    functions.push_back(GetEdgeLengthFunction());
    //    functions.push_back(GetGetNumCellsFunction());
    //    functions.push_back(GetGetRes0CellsFunction());
    //    functions.push_back(GetGetRes0CellsVarcharFunction());
    //    functions.push_back(GetGetPentagonsFunction());
    //    functions.push_back(GetGetPentagonsVarcharFunction());
    //    functions.push_back(GetGreatCircleDistanceFunction());
    //
    //    // Regions
    //    functions.push_back(GetCellsToMultiPolygonWktFunction());
    //    functions.push_back(GetCellsToMultiPolygonWkbFunction());
    //    functions.push_back(GetPolygonWktToCellsFunction());
    //    functions.push_back(GetPolygonWktToCellsVarcharFunction());
    //    functions.push_back(GetPolygonWkbToCellsFunction());
    //    functions.push_back(GetPolygonWkbToCellsVarcharFunction());
    //    functions.push_back(GetPolygonWktToCellsExperimentalFunction());
    //    functions.push_back(GetPolygonWktToCellsExperimentalVarcharFunction());
    //    functions.push_back(GetPolygonWkbToCellsExperimentalFunction());
    //    functions.push_back(GetPolygonWkbToCellsExperimentalVarcharFunction());

    return std::make_pair(functions, functionSets);
  }

private:
  // Indexing
  static duckdb_scalar_function GetLatLngToCellFunction();
  static duckdb_scalar_function GetLatLngToCellVarcharFunction();
  static duckdb_scalar_function_set GetCellToLatFunction();
  static duckdb_scalar_function_set GetCellToLngFunction();
  //  static CreateScalarFunctionInfo GetCellToLatLngFunction();
  //  static CreateScalarFunctionInfo GetCellToBoundaryWktFunction();
  //  static CreateScalarFunctionInfo GetCellToBoundaryWkbFunction();

  // Inspection
  //  static CreateScalarFunctionInfo GetGetResolutionFunction();
  //  static CreateScalarFunctionInfo GetGetBaseCellNumberFunction();
  static duckdb_scalar_function GetStringToH3Function();
  static duckdb_scalar_function_set GetH3ToStringFunction();
  //  static CreateScalarFunctionInfo GetIsValidIndexFunctions();
  //  static CreateScalarFunctionInfo GetIsValidCellFunctions();
  //  static CreateScalarFunctionInfo GetIsResClassIIIFunction();
  //  static CreateScalarFunctionInfo GetIsPentagonFunction();
  //  static CreateScalarFunctionInfo GetGetIcosahedronFacesFunction();
  //  static CreateScalarFunctionInfo GetGetIndexDigitFunction();
  //  static CreateScalarFunctionInfo GetConstructCellFunction();
  //  static CreateScalarFunctionInfo GetConstructCellVarcharFunction();
  //
  //  // Hierarchy
  //  static CreateScalarFunctionInfo GetCellToParentFunction();
  //  static CreateScalarFunctionInfo GetCellToChildrenFunction();
  //  static CreateScalarFunctionInfo GetCellToChildrenSizeFunction();
  //  static CreateScalarFunctionInfo GetCellToCenterChildFunction();
  //  static CreateScalarFunctionInfo GetCellToChildPosFunction();
  //  static CreateScalarFunctionInfo GetChildPosToCellFunction();
  //  static CreateScalarFunctionInfo GetCompactCellsFunction();
  //  static CreateScalarFunctionInfo GetUncompactCellsFunction();
  //
  //  // Traversal
  //  static CreateScalarFunctionInfo GetGridDiskFunction();
  //  static CreateScalarFunctionInfo GetGridDiskDistancesFunction();
  //  static CreateScalarFunctionInfo GetGridDiskUnsafeFunction();
  //  static CreateScalarFunctionInfo GetGridDiskDistancesUnsafeFunction();
  //  static CreateScalarFunctionInfo GetGridDiskDistancesSafeFunction();
  //  static CreateScalarFunctionInfo GetGridRingFunction();
  //  static CreateScalarFunctionInfo GetGridRingUnsafeFunction();
  //  static CreateScalarFunctionInfo GetGridPathCellsFunction();
  //  static CreateScalarFunctionInfo GetGridDistanceFunction();
  //  static CreateScalarFunctionInfo GetMaxGridDiskSizeFunction();
  //  static CreateScalarFunctionInfo GetCellToLocalIjFunction();
  //  static CreateScalarFunctionInfo GetLocalIjToCellFunction();
  //
  //  // Directed edge
  //  static CreateScalarFunctionInfo GetAreNeighborCellsFunction();
  //  static CreateScalarFunctionInfo GetCellsToDirectedEdgeFunction();
  //  static CreateScalarFunctionInfo GetIsValidDirectedEdgeFunctions();
  //  static CreateScalarFunctionInfo GetGetDirectedEdgeOriginFunction();
  //  static CreateScalarFunctionInfo GetGetDirectedEdgeDestinationFunction();
  //  static CreateScalarFunctionInfo GetDirectedEdgeToCellsFunction();
  //  static CreateScalarFunctionInfo GetOriginToDirectedEdgesFunction();
  //  static CreateScalarFunctionInfo GetDirectedEdgeToBoundaryWktFunction();
  //  static CreateScalarFunctionInfo GetDirectedEdgeToBoundaryWkbFunction();
  //  static CreateScalarFunctionInfo GetReverseDirectedEdgeFunction();
  //
  //  // Vertex
  //  static CreateScalarFunctionInfo GetCellToVertexFunction();
  //  static CreateScalarFunctionInfo GetCellToVertexesFunction();
  //  static CreateScalarFunctionInfo GetVertexToLatFunction();
  //  static CreateScalarFunctionInfo GetVertexToLngFunction();
  //  static CreateScalarFunctionInfo GetVertexToLatLngFunction();
  //  static CreateScalarFunctionInfo GetIsValidVertexFunctions();
  //
  //  // Misc
  //  static CreateScalarFunctionInfo GetGetHexagonAreaAvgFunction();
  //  static CreateScalarFunctionInfo GetCellAreaFunction();
  //  static CreateScalarFunctionInfo GetGetHexagonEdgeLengthAvgFunction();
  //  static CreateScalarFunctionInfo GetEdgeLengthFunction();
  //  static CreateScalarFunctionInfo GetGetNumCellsFunction();
  //  static CreateScalarFunctionInfo GetGetRes0CellsFunction();
  //  static CreateScalarFunctionInfo GetGetRes0CellsVarcharFunction();
  //  static CreateScalarFunctionInfo GetGetPentagonsFunction();
  //  static CreateScalarFunctionInfo GetGetPentagonsVarcharFunction();
  //  static CreateScalarFunctionInfo GetGreatCircleDistanceFunction();
  //
  //  // Regions
  //  static CreateScalarFunctionInfo GetCellsToMultiPolygonWktFunction();
  //  static CreateScalarFunctionInfo GetCellsToMultiPolygonWkbFunction();
  //  static CreateScalarFunctionInfo GetPolygonWktToCellsFunction();
  //  static CreateScalarFunctionInfo GetPolygonWktToCellsVarcharFunction();
  //  static CreateScalarFunctionInfo GetPolygonWkbToCellsFunction();
  //  static CreateScalarFunctionInfo GetPolygonWkbToCellsVarcharFunction();
  //  static CreateScalarFunctionInfo
  //  GetPolygonWktToCellsExperimentalFunction(); static
  //  CreateScalarFunctionInfo
  //  GetPolygonWktToCellsExperimentalVarcharFunction();
  //  static CreateScalarFunctionInfo
  //  GetPolygonWkbToCellsExperimentalFunction(); static
  //  CreateScalarFunctionInfo
  //  GetPolygonWkbToCellsExperimentalVarcharFunction();
};

} // namespace h3duckdb
