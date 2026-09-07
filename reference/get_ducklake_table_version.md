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

A lazy table (class `tbl_ducklake`) that works with dplyr verbs. Like
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
collecting it restores stored column labels.

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

## See also

Other time travel:
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md),
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md),
[`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md),
[`plot_table_changes()`](https://tgerke.github.io/ducklake-r/reference/plot_table_changes.md),
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)

## Examples

``` r
lake_dir <- tempfile("version_lake_")
dir.create(lake_dir)
attach_ducklake("version_lake", lake_path = lake_dir)
create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "orders")

rows_insert(
  get_ducklake_table("orders"),
  data.frame(id = 4L, amount = 40),
  by = "id"
)

# Get available snapshots
snapshots <- list_table_snapshots("orders")

# Query the first snapshot version
get_ducklake_table_version("orders", snapshots$snapshot_id[1]) |>
  dplyr::collect()
#> # A tibble: 3 × 2
#>      id amount
#>   <int>  <dbl>
#> 1     1     10
#> 2     2     20
#> 3     3     30

detach_ducklake("version_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
