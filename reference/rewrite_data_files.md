# Rewrite data files with many deletes

Rewrites Parquet files whose rows have mostly been deleted. Deletes in
DuckLake are recorded in separate delete files; heavily-deleted data
files slow reads down until they are rewritten without the dead rows.

## Usage

``` r
rewrite_data_files(
  ducklake_name = NULL,
  table_name = NULL,
  delete_threshold = NULL
)
```

## Arguments

- ducklake_name:

  Name of the attached DuckLake catalog. If `NULL`, the current database
  is used.

- table_name:

  Optional table name. When provided, only that table's files are
  rewritten.

- delete_threshold:

  Optional fraction of deleted rows (between 0 and

  1.  above which a file is rewritten. DuckLake's default is 0.95.

## Value

A data frame with one row per output file (columns `schema_name`,
`table_name`, `files_processed`, `files_created`).

## Details

The rewritten originals are scheduled for deletion once no snapshot
references them; run
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md)
to remove them.

## See also

[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)

Other maintenance:
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Rewrite any file that is at least half deleted
rewrite_data_files("my_lake", delete_threshold = 0.5)

# Just one table, with DuckLake's default threshold
rewrite_data_files(table_name = "events")
} # }
```
