[![tests](https://github.com/isaacbrodsky/h3-duckdb/actions/workflows/tests.yml/badge.svg)](https://github.com/isaacbrodsky/h3-duckdb/actions/workflows/tests.yml)

To build, type 
```sh
make duckdb_release release
```

To run, run the bundled `duckdb` shell:
```sh
./duckdb/build/release/duckdb 
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

- `h3_is_valid_cell`
- `h3_cell_to_parent`
- `h3_latlng_to_cell`

# License

h3-duckdb Copyright 2022 Isaac Brodsky. Licensed under the [Apache 2.0 License](./LICENSE).

[H3](https://github.com/uber/h3) Copyright 2018 Uber Technologies Inc. (Apache 2.0 License)

DGGRID Copyright (c) 2015 Southern Oregon University

[DuckDB](https://github.com/duckdb/duckdb) Copyright 2018-2022 Stichting DuckDB Foundation (MIT License)

Build system adapted from [sqlitescanner](https://github.com/duckdblabs/sqlitescanner) Copyright 2018-2022 DuckDB Labs BV (MIT License)
