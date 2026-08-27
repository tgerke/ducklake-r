# List available snapshots for a table

Retrieves information about available snapshots/versions for a table.

## Usage

``` r
list_table_snapshots(table_name = NULL, ducklake_name = NULL, conn = NULL)
```

## Arguments

- table_name:

  The name of the table to query

- ducklake_name:

  The name of the ducklake (database) to query. If NULL, will attempt to
  infer from current database.

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

A data frame with snapshot information (version, timestamp, etc.)

## Details

This function queries the snapshot history of a table, showing available
versions and their timestamps. This is useful for understanding what
historical versions are available for time-travel queries.

## See also

Other time travel:
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md),
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md),
[`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md),
[`plot_table_changes()`](https://tgerke.github.io/ducklake-r/reference/plot_table_changes.md),
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)

## Examples

``` r
lake_dir <- tempfile("snaplist_lake_")
dir.create(lake_dir)
attach_ducklake("snaplist_lake", lake_path = lake_dir)
create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "orders")

# List all snapshots for a table
list_table_snapshots("orders")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-08-27 21:47:10              1
#>                                          changes author commit_message
#> 1 tables_created, inlined_insert, main.orders, 1   <NA>           <NA>
#>   commit_extra_info
#> 1              <NA>

detach_ducklake("snaplist_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
