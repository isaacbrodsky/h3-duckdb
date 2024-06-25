[![Extension Test](https://github.com/isaacbrodsky/h3-duckdb/actions/workflows/test.yml/badge.svg)](https://github.com/isaacbrodsky/h3-duckdb/actions/workflows/test.yml)
[![DuckDB Version](https://img.shields.io/static/v1?label=duckdb&message=v1.0.0&color=blue)](https://github.com/duckdb/duckdb/releases/tag/v1.0.0)
[![H3 Version](https://img.shields.io/static/v1?label=h3&message=v4.1.0&color=blue)](https://github.com/uber/h3/releases/tag/v4.1.0)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

This is a [DuckDB](https://duckdb.org) extension that adds support for the [H3 discrete global grid system](https://github.com/uber/h3/), so you can index points and geometries to hexagons in SQL.

# Install

Run DuckDB with the unsigned option:
```sh
duckdb -unsigned
```

Load the extension:
```SQL
INSTALL h3 FROM 'https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev';
LOAD h3;
```

Test running an H3 function:
```SQL
SELECT h3_cell_to_latlng('822d57fffffffff');
```

Or, using the integer API, which generally has better performance:
```SQL
SELECT h3_cell_to_latlng(586265647244115967);
```

## Download

If you want to directly download the latest version of the extension: [Linux AMD64](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v1.0.0/linux_amd64/h3.duckdb_extension.gz) [Linux AMD64 GCC4](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v1.0.0/linux_amd64_gcc4/h3.duckdb_extension.gz) [Linux Arm64](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v1.0.0/linux_arm64/h3.duckdb_extension.gz) [OSX AMD64](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v1.0.0/osx_amd64/h3.duckdb_extension.gz) [OSX Arm64](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v1.0.0/osx_arm64/h3.duckdb_extension.gz) [wasm eh](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v1.0.0/wasm_eh/h3.duckdb_extension.wasm) [wasm mvp](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v1.0.0/wasm_mvp/h3.duckdb_extension.wasm) [wasm threads](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v1.0.0/wasm_threads/h3.duckdb_extension.wasm) [Windows AMD64](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v1.0.0/windows_amd64/h3.duckdb_extension.gz)

# Implemented functions

This extension implements the entire [H3 API](https://h3geo.org/docs/api/indexing). The full list of functions is below.

All functions support H3 indexes specified as `UBIGINT` (`uint64`) or `BIGINT` (`int64`),
but the unsigned one is preferred and is returned when the extension can't detect which
one to use. The unsigned and signed APIs are identical. Many functions also support
`VARCHAR` H3 index input and output.

### Full list of functions

| Function | Notes | Description
| --: | --- | ---
| `h3_latlng_to_cell` | [u](#fu) | Convert latitude/longitude coordinate to cell ID
| `h3_cell_to_lat` | [v](#fv) | Convert cell ID to latitude
| `h3_cell_to_lng` | [v](#fv) | Convert cell ID to longitude
| `h3_cell_to_latlng` | [v](#fv) | Convert cell ID to latitude/longitude
| `h3_cell_to_boundary_wkt` | [v](#fv) | Convert cell ID to cell boundary
| `h3_get_resolution` | [v](#fv) | Get resolution number of cell ID
| `h3_get_base_cell_number` | [v](#fv) | Get base cell number of cell ID
| `h3_string_to_h3` | [u](#fu) | Convert VARCHAR cell ID to UBIGINT
| `h3_h3_to_string` | [i](#fi) | Convert BIGINT or UBIGINT cell ID to VARCHAR
| `h3_is_valid_cell` | [v](#fv) | True if this is a valid cell ID
| `h3_is_res_class_iii` | [v](#fv) | True if the cell's resolution is class III
| `h3_is_pentagon` | [v](#fv) | True if the cell is a pentagon
| `h3_get_icosahedron_faces` | [v](#fv) | List of icosahedron face IDs the cell is on
| `h3_cell_to_parent` | [i](#fi) | Get coarser cell for a cell
| `h3_cell_to_children` | [i](#fi) | Get finer cells for a cell
| `h3_cell_to_center_child` | [i](#fi) | Get the center finer cell for a cell
| `h3_cell_to_child_pos` | [i](#fi) | Get a sub-indexing number for a cell inside a parent
| `h3_child_pos_to_cell` | [i](#fi) | Convert parent and sub-indexing number to a cell ID
| `h3_compact_cells` | [i](#fi) | Convert a set of single-resolution cells to the minimal mixed-resolution set
| `h3_uncompact_cells` | [i](#fi) | Convert a mixed-resolution set to a single-resolution set of cells
| `h3_grid_disk` | [i](#fi) | Find cells within a grid distance
| `h3_grid_disk_distances` | [i](#fi) | Find cells within a grid distance, sorted by distance
| `h3_grid_disk_unsafe` | [i](#fi) | Find cells within a grid distance, with no pentagon distortion
| `h3_grid_disk_distances_unsafe` | [i](#fi) | Find cells within a grid distance, sorted by distance, with no pentagon distortion
| `h3_grid_ring_unsafe` | [i](#fi) | Find cells exactly a grid distance away, with no pentagon distortion
| `h3_grid_path_cells` | [i](#fi) | Find a grid path to connect two cells
| `h3_grid_distance` | [i](#fi) | Find the grid distance between two cells
| `h3_cell_to_local_ij` | [i](#fi) | Convert a cell ID to a local I,J coordinate space
| `h3_local_ij_to_cell` | [i](#fi) | Convert a local I,J coordinate to a cell ID
| `h3_cell_to_vertex` | [i](#fi) | Get the vertex ID for a cell ID and vertex number
| `h3_cell_to_vertexes` | [i](#fi) | Get all vertex IDs for a cell ID
| `h3_vertex_to_lat` | [i](#fi) | Convert a vertex ID to latitude
| `h3_vertex_to_lng` | [i](#fi) | Convert a vertex ID to longitude
| `h3_vertex_to_latlng` | [i](#fi) | Convert a vertex ID to latitude/longitude coordinate
| `h3_is_valid_vertex` | [v](#fv) | True if passed a valid vertex ID
| `h3_is_valid_directed_edge` | [v](#fv) | True if passed a valid directed edge ID
| `h3_origin_to_directed_edges` | [i](#fi) | Get all directed edge IDs for a cell ID
| `h3_directed_edge_to_cells` | [i](#fi) | Convert a directed edge ID to origin/destination cell IDs
| `h3_get_directed_edge_origin` | [i](#fi) | Convert a directed edge ID to origin cell ID
| `h3_get_directed_edge_destination` | [i](#fi) | Convert a directed edge ID to destination cell ID
| `h3_cells_to_directed_edge` | [i](#fi) | Convert an origin/destination pair to directed edge ID
| `h3_are_neighbor_cells` | [i](#fi) | True if the two cell IDs are directly adjacent
| `h3_directed_edge_to_boundary_wkt` | [v](#fv) | Convert directed edge ID to linestring WKT
| `h3_get_hexagon_area_avg` | | Get average area of a hexagon cell at resolution
| `h3_cell_area` | [v](#fv) | Get the area of a cell ID
| `h3_edge_length` | [v](#fv) | Get the length of a directed edge ID
| `h3_get_num_cells` | | Get the number of cells at a resolution
| `h3_get_res0_cells` | [u](#fu) | Get all resolution 0 cells
| `h3_get_pentagons` | [u](#fu) | Get all pentagons at a resolution
| `h3_great_circle_distance` | | Compute the great circle distance between two points (haversine)
| `h3_cells_to_multi_polygon_wkt` | [v](#fv) | Convert a set of cells to multipolygon WKT
| `h3_polygon_wkt_to_cells` | [u](#fu) | Convert polygon WKT to a set of cells

### Notes

* <i id="fv">v</i>: Supports VARCHAR, UBIGINT, and BIGINT input and output.
* <i id="fi">i</i>: Supports UBIGINT and BIGINT input and output. (TODO for these to support VARCHAR too.)
* <i id="fu">u</i>: Supports UBIGINT output only.

# Development

To build, type:
```sh
git submodule update --init
GEN=ninja make release
```

You will need Git, CMake, and a C compiler. The build instructions suggest using `ninja`
because it enables parallelism by default. Using `make` instead is fine, but you will want
to enable the following parallelism option, because building DuckDB can take a very long
time (>=1 hour is not unusual). Run the below replacing `4` with the number of CPU cores
on your machine.

```sh
CMAKE_BUILD_PARALLEL_LEVEL=4 make duckdb_release release
```

To run, run the bundled `duckdb` shell:

```sh
./build/release/duckdb -unsigned
```

Load the extension:

```SQL
load 'build/release/extension/h3/h3.duckdb_extension';
```

To run tests:

```sh
make test
```

To update the submodules to latest upstream, run:

```sh
make update_deps
```

# License

h3-duckdb Copyright 2022 Isaac Brodsky. Licensed under the [Apache 2.0 License](./LICENSE).

[H3](https://github.com/uber/h3) Copyright 2018 Uber Technologies Inc. (Apache 2.0 License)

DGGRID Copyright (c) 2015 Southern Oregon University

[DuckDB](https://github.com/duckdb/duckdb) Copyright 2018-2022 Stichting DuckDB Foundation (MIT License)

[DuckDB extension-template](https://github.com/duckdb/extension-template) Copyright 2018-2022 DuckDB Labs BV (MIT License)
