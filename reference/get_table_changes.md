# Get the changes made to a table between two snapshots

Returns the exact rows that were inserted, deleted, or updated in a
table between two snapshots (inclusive), using DuckLake's data change
feed. Useful for auditing and for change-data-capture style pipelines.

## Usage

``` r
get_table_changes(
  table_name,
  start = NULL,
  end = NULL,
  ducklake_name = NULL,
  conn = NULL
)
```

## Arguments

- table_name:

  The name of the table to inspect, optionally qualified as
  `"schema.table"` (default schema `main`).

- start:

  The first snapshot to include: either a snapshot id (see
  [`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md))
  or a timestamp (POSIXct or character). With `end` also `NULL` (the
  default), the feed covers the table's full history, from its first
  snapshot to its latest.

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
`"delete"`, `"update_preimage"`, or `"update_postimage"`). It also
carries a `ducklake_changes` attribute naming the table, the lake, and
the requested range, which
[`view_table_changes()`](https://tgerke.github.io/ducklake-r/reference/view_table_changes.md)
reads; the attribute survives dplyr verbs on the lazy table and is
dropped by
[`collect()`](https://dplyr.tidyverse.org/reference/compute.html).

## Details

Both bounds must be given or both left `NULL`. When given, they must be
of the same kind: two snapshot ids or two timestamps. POSIXct bounds are
converted to UTC, matching the snapshot times DuckLake records;
character bounds are passed through as-is and must already be in UTC.
Bounds before the lake's first snapshot are rejected by DuckLake, so
prefer snapshot times from
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md).
Updates appear as two rows – the row as it looked before the change
(`update_preimage`) and after it (`update_postimage`).

The feed is read with the table's schema as of the end snapshot: a
column added inside the range is `NA` in rows from before it existed,
and a renamed column appears under its newer name. A table rebuilt by
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
or
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)
gets a new table id, and the feed covers the current id only, so changes
from before the rebuild are not in it.

This wraps DuckLake's
[`table_changes()`](https://ducklake.select/docs/stable/duckdb/advanced_features/data_change_feed)
function.

## See also

[`view_table_changes()`](https://tgerke.github.io/ducklake-r/reference/view_table_changes.md)
to browse the feed interactively,
[`plot_table_changes()`](https://tgerke.github.io/ducklake-r/reference/plot_table_changes.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md),
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md)

Other time travel:
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md),
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md),
[`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md),
[`plot_table_changes()`](https://tgerke.github.io/ducklake-r/reference/plot_table_changes.md),
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md),
[`view_table_changes()`](https://tgerke.github.io/ducklake-r/reference/view_table_changes.md)

## Examples

``` r
lake_dir <- tempfile("changes_lake_")
dir.create(lake_dir)
attach_ducklake("changes_lake", lake_path = lake_dir)
create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "orders")

rows_delete(
  get_ducklake_table("orders"),
  data.frame(id = 1L),
  by = "id"
)
snaps <- list_table_snapshots("orders")

# Everything that ever happened to the table
get_table_changes("orders") |> dplyr::collect()
#> # A tibble: 4 × 5
#>   snapshot_id rowid change_type    id amount
#>         <dbl> <dbl> <chr>       <int>  <dbl>
#> 1           1     0 insert          1     10
#> 2           1     1 insert          2     20
#> 3           1     2 insert          3     30
#> 4           2     0 delete          1     10

# What changed in the most recent snapshot?
latest <- max(snaps$snapshot_id)
get_table_changes("orders", latest, latest) |> dplyr::collect()
#> # A tibble: 1 × 5
#>   snapshot_id rowid change_type    id amount
#>         <dbl> <dbl> <chr>       <int>  <dbl>
#> 1           2     0 delete          1     10

# Every change across the table's full history, by timestamp
get_table_changes(
  "orders",
  min(snaps$snapshot_time), max(snaps$snapshot_time) + 1
) |>
  dplyr::filter(change_type == "delete") |>
  dplyr::collect()
#> # A tibble: 1 × 5
#>   snapshot_id rowid change_type    id amount
#>         <dbl> <dbl> <chr>       <int>  <dbl>
#> 1           2     0 delete          1     10

detach_ducklake("changes_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
