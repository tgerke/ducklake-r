# List the data files backing a DuckLake table

Returns the Parquet data files (and any delete files) that make up a
table, optionally as of a past snapshot.

## Usage

``` r
list_ducklake_files(
  table_name,
  schema_name = NULL,
  snapshot_version = NULL,
  snapshot_time = NULL,
  ducklake_name = NULL
)
```

## Arguments

- table_name:

  The table whose files to list.

- schema_name:

  Optional schema containing the table (defaults to the lake's `main`
  schema).

- snapshot_version:

  Optional snapshot id: list the files as of that snapshot. Mutually
  exclusive with `snapshot_time`.

- snapshot_time:

  Optional POSIXct or UTC timestamp string: list the files as of that
  moment. Mutually exclusive with `snapshot_version`.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

A data frame with one row per data file, including `data_file`,
`data_file_size_bytes`, and the associated `delete_file` columns (`NA`
when a file has no deletes).

## Details

Wraps `ducklake_list_files()`. For per-table file counts and sizes
across the whole lake, see
[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md);
for a picture of storage layout, see
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md).

## See also

[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md)

Other maintenance:
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Files behind a table right now
list_ducklake_files("readings")

# Files as of an earlier snapshot
list_ducklake_files("readings", snapshot_version = 3)
} # }
```
