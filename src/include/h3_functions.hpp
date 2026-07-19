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

    // Hierarchy
    functionSets.push_back(GetCellToParentFunction());
    functionSets.push_back(GetCellToChildrenFunction());
    functionSets.push_back(GetCellToChildrenSizeFunction());
    functionSets.push_back(GetCellToCenterChildFunction());
    functionSets.push_back(GetCellToChildPosFunction());
    functionSets.push_back(GetChildPosToCellFunction());
    functionSets.push_back(GetCompactCellsFunction());
    functionSets.push_back(GetUncompactCellsFunction());

    // Traversal
    functionSets.push_back(GetGridDiskFunction());
    functionSets.push_back(GetGridDiskDistancesFunction());
    functionSets.push_back(GetGridDiskUnsafeFunction());
    functionSets.push_back(GetGridDiskDistancesUnsafeFunction());
    functionSets.push_back(GetGridDiskDistancesSafeFunction());
    functionSets.push_back(GetGridRingFunction());
    functionSets.push_back(GetGridRingUnsafeFunction());
    functionSets.push_back(GetGridPathCellsFunction());
    functionSets.push_back(GetGridDistanceFunction());
    functions.push_back(GetMaxGridDiskSizeFunction());
    functionSets.push_back(GetCellToLocalIjFunction());
    functionSets.push_back(GetLocalIjToCellFunction());

    // Directed edge
    functionSets.push_back(GetAreNeighborCellsFunction());
    functionSets.push_back(GetCellsToDirectedEdgeFunction());
    functionSets.push_back(GetIsValidDirectedEdgeFunctions());
    functionSets.push_back(GetGetDirectedEdgeOriginFunction());
    functionSets.push_back(GetGetDirectedEdgeDestinationFunction());
    functionSets.push_back(GetDirectedEdgeToCellsFunction());
    functionSets.push_back(GetOriginToDirectedEdgesFunction());
    functionSets.push_back(GetDirectedEdgeToBoundaryWktFunction());
    functionSets.push_back(GetDirectedEdgeToBoundaryWkbFunction());
    functionSets.push_back(GetReverseDirectedEdgeFunction());

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

    // Regions
    functionSets.push_back(GetCellsToMultiPolygonWktFunction());
    functionSets.push_back(GetCellsToMultiPolygonWkbFunction());
    functions.push_back(GetPolygonWktToCellsFunction());
    functions.push_back(GetPolygonWktToCellsVarcharFunction());
    functions.push_back(GetPolygonWkbToCellsFunction());
    functions.push_back(GetPolygonWkbToCellsVarcharFunction());
    functionSets.push_back(GetPolygonWktToCellsExperimentalFunction());
    functionSets.push_back(GetPolygonWktToCellsExperimentalVarcharFunction());
    functionSets.push_back(GetPolygonWkbToCellsExperimentalFunction());
    functionSets.push_back(GetPolygonWkbToCellsExperimentalVarcharFunction());

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

  // Hierarchy
  static duckdb_scalar_function_set GetCellToParentFunction();
  static duckdb_scalar_function_set GetCellToChildrenFunction();
  static duckdb_scalar_function_set GetCellToChildrenSizeFunction();
  static duckdb_scalar_function_set GetCellToCenterChildFunction();
  static duckdb_scalar_function_set GetCellToChildPosFunction();
  static duckdb_scalar_function_set GetChildPosToCellFunction();
  static duckdb_scalar_function_set GetCompactCellsFunction();
  static duckdb_scalar_function_set GetUncompactCellsFunction();

  // Traversal
  static duckdb_scalar_function_set GetGridDiskFunction();
  static duckdb_scalar_function_set GetGridDiskDistancesFunction();
  static duckdb_scalar_function_set GetGridDiskUnsafeFunction();
  static duckdb_scalar_function_set GetGridDiskDistancesUnsafeFunction();
  static duckdb_scalar_function_set GetGridDiskDistancesSafeFunction();
  static duckdb_scalar_function_set GetGridRingFunction();
  static duckdb_scalar_function_set GetGridRingUnsafeFunction();
  static duckdb_scalar_function_set GetGridPathCellsFunction();
  static duckdb_scalar_function_set GetGridDistanceFunction();
  static duckdb_scalar_function GetMaxGridDiskSizeFunction();
  static duckdb_scalar_function_set GetCellToLocalIjFunction();
  static duckdb_scalar_function_set GetLocalIjToCellFunction();

  // Directed edge
  static duckdb_scalar_function_set GetAreNeighborCellsFunction();
  static duckdb_scalar_function_set GetCellsToDirectedEdgeFunction();
  static duckdb_scalar_function_set GetIsValidDirectedEdgeFunctions();
  static duckdb_scalar_function_set GetGetDirectedEdgeOriginFunction();
  static duckdb_scalar_function_set GetGetDirectedEdgeDestinationFunction();
  static duckdb_scalar_function_set GetDirectedEdgeToCellsFunction();
  static duckdb_scalar_function_set GetOriginToDirectedEdgesFunction();
  static duckdb_scalar_function_set GetDirectedEdgeToBoundaryWktFunction();
  static duckdb_scalar_function_set GetDirectedEdgeToBoundaryWkbFunction();
  static duckdb_scalar_function_set GetReverseDirectedEdgeFunction();

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

  // Regions
  static duckdb_scalar_function_set GetCellsToMultiPolygonWktFunction();
  static duckdb_scalar_function_set GetCellsToMultiPolygonWkbFunction();
  static duckdb_scalar_function GetPolygonWktToCellsFunction();
  static duckdb_scalar_function GetPolygonWktToCellsVarcharFunction();
  static duckdb_scalar_function GetPolygonWkbToCellsFunction();
  static duckdb_scalar_function GetPolygonWkbToCellsVarcharFunction();
  static duckdb_scalar_function_set GetPolygonWktToCellsExperimentalFunction();
  static duckdb_scalar_function_set
  GetPolygonWktToCellsExperimentalVarcharFunction();
  static duckdb_scalar_function_set GetPolygonWkbToCellsExperimentalFunction();
  static duckdb_scalar_function_set
  GetPolygonWkbToCellsExperimentalVarcharFunction();
};

} // namespace h3duckdb
