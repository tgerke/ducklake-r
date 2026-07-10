# Get file statistics for the tables in a lake

Returns per-table storage statistics from the DuckLake catalog: how many
Parquet data files each table has and their total size, plus the same
for delete files.

## Usage

``` r
get_table_info(table_name = NULL, ducklake_name = NULL, conn = NULL)
```

## Arguments

- table_name:

  Optional table name. When provided, only that table's row is returned.

- ducklake_name:

  Name of the attached DuckLake catalog. If `NULL`, the current database
  is used.

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

A data frame with one row per table and columns `table_name`,
`schema_id`, `table_id`, `table_uuid`, `file_count`, `file_size_bytes`,
`delete_file_count`, and `delete_file_size_bytes`.

## Details

These statistics are the raw material for storage maintenance decisions:
many small files are worth compacting with
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
and a growing delete-file share is a sign to run
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md).
Rows that are still inlined in the catalog (see
[`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md))
are not in any Parquet file yet, so small recent writes may not show up
in the counts until
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md)
writes them out.

This wraps DuckLake's `ducklake_table_info()` function.

## See also

[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

Other maintenance:
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`list_ducklake_files()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_files.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# File statistics for every table in the lake
get_table_info()

# Just one table
get_table_info("my_table")
} # }
```
