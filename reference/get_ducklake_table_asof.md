# Query a table at a specific timestamp (time travel)

Retrieves data from a DuckLake table as it existed at a specific point
in time using DuckLake's AT (TIMESTAMP =\> ...) syntax.

## Usage

``` r
get_ducklake_table_asof(table_name, timestamp, conn = NULL)
```

## Arguments

- table_name:

  The name of the table to query

- timestamp:

  A POSIXct timestamp (converted to UTC, which is how DuckLake records
  snapshot times) or character string in ISO 8601 format already in UTC
  (e.g., "2024-01-15 10:30:00")

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

A lazy table (class `tbl_ducklake`) that works with dplyr verbs. Like
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
collecting it restores stored column labels.

## Details

DuckLake supports time-travel queries, allowing you to query historical
data as it existed at a specific timestamp. This uses the syntax:
`SELECT * FROM table AT (TIMESTAMP => 'timestamp')`

This is useful for:

- Auditing changes over time

- Recovering accidentally deleted or modified data

- Comparing data states across different time points

- Regulatory compliance and data lineage documentation

The timestamp must be within the range of available snapshots for the
table. Use
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md)
to see available snapshot times.

**Important**: When querying at a snapshot's exact timestamp, you may
need to add a small time buffer (e.g., +1 second) to ensure the snapshot
is found. This is because the time-travel query looks for snapshots
created at or before the specified timestamp.

## See also

Other time travel:
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md),
[`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md),
[`plot_table_changes()`](https://tgerke.github.io/ducklake-r/reference/plot_table_changes.md),
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)

## Examples

``` r
lake_dir <- tempfile("asof_lake_")
dir.create(lake_dir)
attach_ducklake("asof_lake", lake_path = lake_dir)
create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "orders")

rows_insert(
  get_ducklake_table("orders"),
  data.frame(id = 4L, amount = 40),
  by = "id"
)

# Query data at a specific snapshot time
snapshots <- list_table_snapshots("orders")
# Add 1 second to ensure the snapshot is found
get_ducklake_table_asof("orders", snapshots$snapshot_time[1] + 1) |>
  dplyr::summarise(total = sum(amount)) |>
  dplyr::collect()
#> # A tibble: 1 × 1
#>   total
#>   <dbl>
#> 1   100

detach_ducklake("asof_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
