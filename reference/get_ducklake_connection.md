# Get the current DuckLake connection

Returns the DuckDB connection used by DuckLake. This is always
duckplyr's default singleton connection, which provides critical setup
(temp directory configuration, R function loading, and macro
registration).

## Usage

``` r
get_ducklake_connection()
```

## Value

A DuckDB connection object

## Note

This function uses `duckplyr:::get_default_duckdb_connection()`. While
this accesses an unexported function, it is necessary for proper
duckplyr integration. See the duckplyr source for details:
<https://github.com/tidyverse/duckplyr/blob/main/R/relational-duckdb.R>
