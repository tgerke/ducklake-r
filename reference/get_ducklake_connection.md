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
fallback when no connection has been explicitly set. While this accesses
an unexported function, it is necessary for proper duckplyr integration
as duckplyr's connection provides critical setup (singleton pattern,
temp directory configuration, R function loading, and macro
registration) that cannot be easily replicated. See the duckplyr source
for details:
<https://github.com/tidyverse/duckplyr/blob/main/R/relational-duckdb.R>
