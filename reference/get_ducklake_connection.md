# Get the current DuckLake connection

This function retrieves the active DuckLake connection. If no connection
has been explicitly set via
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md),
it falls back to duckplyr's default DuckDB connection for seamless
integration.

## Usage

``` r
get_ducklake_connection()
```

## Value

A DuckDB connection object

## Note

This function uses `duckplyr:::get_default_duckdb_connection()` as a
fallback. While this is an unexported function from duckplyr, it is
necessary for proper integration with the duckplyr ecosystem when no
explicit ducklake connection is set.
