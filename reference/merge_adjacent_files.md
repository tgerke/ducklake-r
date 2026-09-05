# Merge adjacent Parquet files

Compacts small adjacent Parquet files into larger ones. Frequent small
inserts each write their own file; merging keeps file counts down and
scans fast.

## Usage

``` r
merge_adjacent_files(
  ducklake_name = NULL,
  table_name = NULL,
  schema_name = NULL,
  max_compacted_files = NULL,
  min_file_size = NULL,
  max_file_size = NULL
)
```

## Arguments

- ducklake_name:

  Name of the attached DuckLake catalog. If `NULL`, the current database
  is used.

- table_name:

  Optional table name, optionally qualified as `"schema.table"`. When
  provided, only that table is compacted.

- schema_name:

  Optional schema name. When provided, only tables in that schema are
  compacted.

- max_compacted_files:

  Optional cap on the number of compaction operations per table in a
  single call.

- min_file_size:

  Optional minimum file size in bytes; smaller files are excluded from
  merging.

- max_file_size:

  Optional maximum file size in bytes; files at or above this size are
  excluded. Defaults to the lake's target file size.

## Value

A data frame with one row per output file (columns `schema_name`,
`table_name`, `files_processed`, `files_created`).

## Details

Merging does not delete the original small files – they may still be
referenced by older snapshots. They are scheduled for deletion once no
snapshot references them; run
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md)
to remove them.

## See also

[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md)

Other maintenance:
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md),
[`list_ducklake_files()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_files.md),
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
lake_dir <- tempfile("merge_files_lake_")
dir.create(lake_dir)
attach_ducklake("merge_files_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

rows_insert(
  get_ducklake_table("cars"),
  data.frame(mpg = 30, cyl = 4),
  by = "mpg"
)

# Compact the whole lake
merge_adjacent_files()
#> No adjacent files to merge.
#> [1] schema_name     table_name      files_processed files_created  
#> <0 rows> (or 0-length row.names)

# Compact one table, only touching files under 10 MB
merge_adjacent_files(table_name = "cars", max_file_size = 10e6)
#> No adjacent files to merge.
#> [1] schema_name     table_name      files_processed files_created  
#> <0 rows> (or 0-length row.names)

detach_ducklake("merge_files_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
