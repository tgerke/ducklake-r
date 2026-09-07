# Query a table at a specific version/snapshot

Retrieves data from a DuckLake table at a specific snapshot ID using
DuckLake's AT (VERSION =\> ...) syntax.

## Usage

``` r
get_ducklake_table_version(table_name, version, conn = NULL)
```

## Arguments

- table_name:

  The name of the table to query

- version:

  The snapshot_id to query (get this from
  [`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md))

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

A dplyr lazy query object (tbl_lazy) that can be further manipulated
with dplyr verbs

## Details

This function allows you to query a specific snapshot of a table using
its snapshot_id. This uses the syntax:
`SELECT * FROM table AT (VERSION => snapshot_id)`

Each time you create or modify a table within a transaction, DuckLake
creates a new snapshot with a unique snapshot_id. Note that snapshot_id
and schema_version are typically the same value - both represent the
snapshot identifier.

Use `list_table_snapshots(table_name)` to see all available snapshots
and their IDs.

## Examples

``` r
if (FALSE) { # \dontrun{
# Get available snapshots
snapshots <- list_table_snapshots("my_table")

# Query the first snapshot version
get_ducklake_table_version("my_table", snapshots$snapshot_id[1]) |>
  filter(status == "active") |>
  collect()
} # }
```
