[![Extension Test](https://github.com/isaacbrodsky/h3-duckdb/actions/workflows/test.yml/badge.svg)](https://github.com/isaacbrodsky/h3-duckdb/actions/workflows/test.yml)
[![Extension Deployment](https://github.com/isaacbrodsky/h3-duckdb/actions/workflows/_extension_deploy.yml/badge.svg)](https://github.com/isaacbrodsky/h3-duckdb/actions/workflows/_extension_deploy.yml)
[![DuckDB Version](https://img.shields.io/static/v1?label=duckdb&message=v0.10.1&color=blue)](https://github.com/duckdb/duckdb/releases/tag/v0.10.1)
[![H3 Version](https://img.shields.io/static/v1?label=h3&message=v4.1.0&color=blue)](https://github.com/uber/h3/releases/tag/v4.1.0)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

This is a [DuckDB](https://duckdb.org) extension that adds support for the [H3 discrete global grid system](https://github.com/uber/h3/).

Download the latest version of the extension: [Linux AMD64](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v0.10.1/linux_amd64/h3ext.duckdb_extension.gz) [Linux AMD64 GCC4](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v0.10.1/linux_amd64_gcc4/h3ext.duckdb_extension.gz) [Linux Arm64](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v0.10.1/linux_arm64/h3ext.duckdb_extension.gz) [OSX AMD64](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v0.10.1/osx_amd64/h3ext.duckdb_extension.gz) [OSX Arm64](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v0.10.1/osx_arm64/h3ext.duckdb_extension.gz) [wasm eh](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v0.10.1/wasm_eh/h3ext.duckdb_extension.wasm) [wasm mvp](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v0.10.1/wasm_mvp/h3ext.duckdb_extension.wasm) [wasm threads](https://pub-cc26a6fd5d8240078bd0c2e0623393a5.r2.dev/v0.10.1/wasm_threads/h3ext.duckdb_extension.wasm) [Windows AMD64](https://pub-cc26a6fd5d8240078bd0c2es0623393a5.r2.dev/v0.10.1/windows_amd64/h3ext.duckdb_extension.gz)

Run DuckDB with the unsigned option:
```sh
duckdb -unsigned
```

(Note: you must download and ungzip the extension before installing it. Installing from `https` URLs does not seem to work in DuckDB v0.10.1.)

Load the extension:
```SQL
install 'h3ext.duckdb_extension';
load 'h3ext';
```

Test running an H3 function:
```SQL
SELECT h3_cell_to_latlng('822d57fffffffff');
```

Or, using the integer API, which generally has better performance:
```SQL
SELECT h3_cell_to_latlng(cast(586265647244115967 as ubigint));
```

# Implemented functions

This extension implements the entire [H3 API](https://h3geo.org/docs/api/indexing). The full list of functions is below:

- `h3_latlng_to_cell`
- `h3_cell_to_lat`
- `h3_cell_to_lng`
- `h3_cell_to_latlng`
- `h3_cell_to_boundary_wkt`
- `h3_get_resolution`
- `h3_get_base_cell_number`
- `h3_string_to_h3`
- `h3_h3_to_string`
- `h3_is_valid_cell`
- `h3_is_res_class_iii`
- `h3_is_pentagon`
- `h3_get_icosahedron_faces`
- `h3_cell_to_parent`
- `h3_cell_to_children`
- `h3_cell_to_center_child`
- `h3_cell_to_child_pos`
- `h3_child_pos_to_cell`
- `h3_compact_cells`
- `h3_uncompact_cells`
- `h3_grid_disk`
- `h3_grid_disk_distances`
- `h3_grid_disk_unsafe`
- `h3_grid_disk_distances_unsafe`
- `h3_grid_ring_unsafe`
- `h3_grid_path_cells`
- `h3_grid_distance`
- `h3_cell_to_local_ij`
- `h3_local_ij_to_cell`
- `h3_cell_to_vertex`
- `h3_cell_to_vertexes`
- `h3_vertex_to_lat`
- `h3_vertex_to_lng`
- `h3_vertex_to_latlng`
- `h3_is_valid_vertex`
- `h3_is_valid_directed_edge`
- `h3_origin_to_directed_edges`
- `h3_directed_edge_to_cells`
- `h3_get_directed_edge_origin`
- `h3_get_directed_edge_destination`
- `h3_cells_to_directed_edge`
- `h3_are_neighbor_cells`
- `h3_directed_edge_to_boundary_wkt`
- `h3_get_hexagon_area_avg`
- `h3_cell_area`
- `h3_edge_length`
- `h3_get_num_cells`
- `h3_get_res0_cells`
- `h3_get_pentagons`
- `h3_great_circle_distance`
- `h3_cells_to_multi_polygon_wkt`
- `h3_polygon_wkt_to_cells`

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
load 'build/release/extension/h3ext/h3ext.duckdb_extension';
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

Build system adapted from [sqlitescanner](https://github.com/duckdblabs/sqlitescanner) Copyright 2018-2022 DuckDB Labs BV (MIT License)
