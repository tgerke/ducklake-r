# Query a table at a specific version/snapshot

Retrieves data from a DuckDB table at a specific version or snapshot
number.

## Usage

``` r
get_ducklake_table_version(table_name, version, conn = NULL)
```

## Arguments

- table_name:

  The name of the table to query

- version:

  The version or snapshot number to query

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

A dplyr lazy query object (tbl_lazy) that can be further manipulated
with dplyr verbs

## Details

This function allows you to query a specific version/snapshot of a
table. This is particularly useful with Delta Lake or Iceberg tables
that maintain version history.

## Examples

``` r
if (FALSE) { # \dontrun{
# Query version 5 of a table
get_ducklake_table_version("my_table", 5) |>
  filter(status == "active") |>
  collect()
} # }
```
