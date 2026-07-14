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
    functionSets.push_back(GetCellToLatLngFunction());
    functionSets.push_back(GetCellToBoundaryWktFunction());
    functionSets.push_back(GetCellToBoundaryWkbFunction());

    // Inspection
    functionSets.push_back(GetGetResolutionFunction());
    functionSets.push_back(GetGetBaseCellNumberFunction());
    functions.push_back(GetStringToH3Function());
    functionSets.push_back(GetH3ToStringFunction());
    functionSets.push_back(GetIsValidIndexFunction());
    functionSets.push_back(GetIsValidCellFunction());
    functionSets.push_back(GetIsResClassIIIFunction());
    functionSets.push_back(GetIsPentagonFunction());
    functionSets.push_back(GetGetIcosahedronFacesFunction());
    functionSets.push_back(GetGetIndexDigitFunction());
    functionSets.push_back(GetConstructCellFunction());
    functionSets.push_back(GetConstructCellVarcharFunction());
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

    // Vertex
    functionSets.push_back(GetCellToVertexFunction());
    functionSets.push_back(GetCellToVertexesFunction());
    functionSets.push_back(GetVertexToLatFunction());
    functionSets.push_back(GetVertexToLngFunction());
    functionSets.push_back(GetVertexToLatLngFunction());
    functionSets.push_back(GetIsValidVertexFunctions());

    // Misc
    functions.push_back(GetGetHexagonAreaAvgFunction());
    functionSets.push_back(GetCellAreaFunction());
    functions.push_back(GetGetHexagonEdgeLengthAvgFunction());
    functionSets.push_back(GetEdgeLengthFunction());
    functions.push_back(GetGetNumCellsFunction());
    functions.push_back(GetGetRes0CellsFunction());
    functions.push_back(GetGetRes0CellsVarcharFunction());
    functions.push_back(GetGetPentagonsFunction());
    functions.push_back(GetGetPentagonsVarcharFunction());
    functions.push_back(GetGreatCircleDistanceFunction());

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
  static duckdb_scalar_function_set GetCellToLatLngFunction();
  static duckdb_scalar_function_set GetCellToBoundaryWktFunction();
  static duckdb_scalar_function_set GetCellToBoundaryWkbFunction();

  // Inspection
  static duckdb_scalar_function_set GetGetResolutionFunction();
  static duckdb_scalar_function_set GetGetBaseCellNumberFunction();
  static duckdb_scalar_function GetStringToH3Function();
  static duckdb_scalar_function_set GetH3ToStringFunction();
  static duckdb_scalar_function_set GetIsValidIndexFunction();
  static duckdb_scalar_function_set GetIsValidCellFunction();
  static duckdb_scalar_function_set GetIsResClassIIIFunction();
  static duckdb_scalar_function_set GetIsPentagonFunction();
  static duckdb_scalar_function_set GetGetIcosahedronFacesFunction();
  static duckdb_scalar_function_set GetGetIndexDigitFunction();
  static duckdb_scalar_function_set GetConstructCellFunction();
  static duckdb_scalar_function_set GetConstructCellVarcharFunction();

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

  // Vertex
  static duckdb_scalar_function_set GetCellToVertexFunction();
  static duckdb_scalar_function_set GetCellToVertexesFunction();
  static duckdb_scalar_function_set GetVertexToLatFunction();
  static duckdb_scalar_function_set GetVertexToLngFunction();
  static duckdb_scalar_function_set GetVertexToLatLngFunction();
  static duckdb_scalar_function_set GetIsValidVertexFunctions();

  // Misc
  static duckdb_scalar_function GetGetHexagonAreaAvgFunction();
  static duckdb_scalar_function_set GetCellAreaFunction();
  static duckdb_scalar_function GetGetHexagonEdgeLengthAvgFunction();
  static duckdb_scalar_function_set GetEdgeLengthFunction();
  static duckdb_scalar_function GetGetNumCellsFunction();
  static duckdb_scalar_function GetGetRes0CellsFunction();
  static duckdb_scalar_function GetGetRes0CellsVarcharFunction();
  static duckdb_scalar_function GetGetPentagonsFunction();
  static duckdb_scalar_function GetGetPentagonsVarcharFunction();
  static duckdb_scalar_function GetGreatCircleDistanceFunction();

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
