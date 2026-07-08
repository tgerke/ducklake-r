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
if (FALSE) { # \dontrun{
# List all snapshots for a table
list_table_snapshots("my_table")
} # }
```
