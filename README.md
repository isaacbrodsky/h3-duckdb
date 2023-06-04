[![tests](https://github.com/isaacbrodsky/h3-duckdb/actions/workflows/tests.yml/badge.svg)](https://github.com/isaacbrodsky/h3-duckdb/actions/workflows/tests.yml)
[![DuckDB Version](https://img.shields.io/static/v1?label=duckdb&message=v0.8.0&color=blue)](https://github.com/duckdb/duckdb/releases/tag/v0.8.0)
[![H3 Version](https://img.shields.io/static/v1?label=h3&message=v4.1.0&color=blue)](https://github.com/uber/h3/releases/tag/v4.1.0)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

To build, type 
```sh
git submodule update --init
make duckdb_release release
```

To run, run the bundled `duckdb` shell:
```sh
./duckdb/build/release/duckdb -unsigned
```

Then, load the H3 extension like so:
```SQL
LOAD 'build/release/h3.duckdb_extension';
```

Test running an H3 function:
```SQL
SELECT h3_cell_to_parent(cast(586265647244115967 as ubigint), 1);
```

# Implemented functions

- `h3_latlng_to_cell`
- `h3_cell_to_lat`
- `h3_cell_to_lng`
- `h3_get_resolution`
- `h3_get_base_cell_number`
- `h3_string_to_h3`
- `h3_h3_to_string`
- `h3_is_valid_cell`
- `h3_is_res_class_iii`
- `h3_is_pentagon`
- `h3_cell_to_parent`
- `h3_cell_to_children`
- `h3_cell_to_center_child`
- `h3_cell_to_child_pos`
- `h3_child_pos_to_cell`
- `h3_compact_cells`
- `h3_uncompact_cells`
- `h3_grid_disk`

# Development

For a greatly sped up build (enables parallelism), run:

```sh
CMAKE_BUILD_PARALLEL_LEVEL=4 make duckdb_release release
```

To update the submodules to latest upstream, run:

```sh
make update_deps
```

To run tests:

```sh
make test
```

# License

h3-duckdb Copyright 2022 Isaac Brodsky. Licensed under the [Apache 2.0 License](./LICENSE).

[H3](https://github.com/uber/h3) Copyright 2018 Uber Technologies Inc. (Apache 2.0 License)

DGGRID Copyright (c) 2015 Southern Oregon University

[DuckDB](https://github.com/duckdb/duckdb) Copyright 2018-2022 Stichting DuckDB Foundation (MIT License)

Build system adapted from [sqlitescanner](https://github.com/duckdblabs/sqlitescanner) Copyright 2018-2022 DuckDB Labs BV (MIT License)
