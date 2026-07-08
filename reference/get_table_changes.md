# Get the changes made to a table between two snapshots

Returns the exact rows that were inserted, deleted, or updated in a
table between two snapshots (inclusive), using DuckLake's data change
feed. Useful for auditing and for change-data-capture style pipelines.

## Usage

``` r
get_table_changes(table_name, start, end, ducklake_name = NULL, conn = NULL)
```

## Arguments

- table_name:

  The name of the table to inspect.

- start:

  The first snapshot to include: either a snapshot id (see
  [`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md))
  or a timestamp (POSIXct or character).

- end:

  The last snapshot to include, in the same form as `start`.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

A dplyr lazy query object (tbl_lazy). In addition to the table's own
columns it carries `snapshot_id` (the snapshot that made the change),
`rowid` (the changed row's identifier), and `change_type` (`"insert"`,
`"delete"`, `"update_preimage"`, or `"update_postimage"`).

## Details

Both bounds must be of the same kind: two snapshot ids or two
timestamps. POSIXct bounds are converted to UTC, matching the snapshot
times DuckLake records; character bounds are passed through as-is and
must already be in UTC. Bounds before the lake's first snapshot are
rejected by DuckLake, so prefer snapshot times from
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md).
Updates appear as two rows – the row as it looked before the change
(`update_preimage`) and after it (`update_postimage`).

This wraps DuckLake's
[`table_changes()`](https://ducklake.select/docs/stable/duckdb/advanced_features/data_change_feed)
function.

## See also

[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md),
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md)

Other time travel:
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md),
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md),
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# What changed in snapshot 3?
get_table_changes("orders", 3, 3) |> dplyr::collect()

# Every change across the table's full history, by timestamp
snaps <- list_table_snapshots("orders")
get_table_changes(
  "orders",
  min(snaps$snapshot_time), max(snaps$snapshot_time) + 1
) |>
  dplyr::filter(change_type == "delete") |>
  dplyr::collect()
} # }
```
