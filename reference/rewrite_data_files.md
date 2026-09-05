# Rewrite data files with many deletes

Rewrites Parquet files whose rows have mostly been deleted. Deletes in
DuckLake are recorded in separate delete files; heavily-deleted data
files slow reads down until they are rewritten without the dead rows.

## Usage

``` r
rewrite_data_files(
  ducklake_name = NULL,
  table_name = NULL,
  delete_threshold = NULL,
  schema_name = NULL
)
```

## Arguments

- ducklake_name:

  Name of the attached DuckLake catalog. If `NULL`, the current database
  is used.

- table_name:

  Optional table name, optionally qualified as `"schema.table"`. When
  provided, only that table's files are rewritten.

- delete_threshold:

  Optional fraction of deleted rows (between 0 and

  1.  above which a file is rewritten. DuckLake's default is 0.95.

- schema_name:

  Optional schema containing `table_name`.

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
[`list_ducklake_files()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_files.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md)

## Examples

``` r
lake_dir <- tempfile("rewrite_lake_")
dir.create(lake_dir)
attach_ducklake("rewrite_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

rows_delete(
  get_ducklake_table("cars"),
  data.frame(gear = 3),
  by = "gear"
)

# Rewrite any file that is at least half deleted
rewrite_data_files("rewrite_lake", delete_threshold = 0.5)
#> No files needed rewriting.
#> [1] schema_name     table_name      files_processed files_created  
#> <0 rows> (or 0-length row.names)

# Just one table, with DuckLake's default threshold
rewrite_data_files(table_name = "cars")
#> No files needed rewriting.
#> [1] schema_name     table_name      files_processed files_created  
#> <0 rows> (or 0-length row.names)

detach_ducklake("rewrite_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
