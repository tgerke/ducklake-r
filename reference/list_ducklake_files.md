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

  The table whose files to list, optionally qualified as
  `"schema.table"`.

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
lake_dir <- tempfile("listfiles_lake_")
dir.create(lake_dir)
attach_ducklake("listfiles_lake", lake_path = lake_dir)
create_table(mtcars, "cars")
rows_insert(
  get_ducklake_table("cars"),
  data.frame(mpg = 30, cyl = 4),
  by = "mpg"
)

# Files behind a table right now
list_ducklake_files("cars")
#>                                                                                                     data_file
#> 1 /tmp/RtmpiKJyX7/listfiles_lake_18dd36c83662/main/cars/ducklake-01a09110-6eed-75b6-ba61-86c0a4ba5e46.parquet
#>   data_file_size_bytes data_file_footer_size data_file_encryption_key
#> 1                 2911                  1128                     NULL
#>   delete_file delete_file_size_bytes delete_file_footer_size
#> 1        <NA>                     NA                      NA
#>   delete_file_encryption_key
#> 1                       NULL

# Files as of an earlier snapshot
first <- min(list_table_snapshots("cars")$snapshot_id)
list_ducklake_files("cars", snapshot_version = first)
#>                                                                                                     data_file
#> 1 /tmp/RtmpiKJyX7/listfiles_lake_18dd36c83662/main/cars/ducklake-01a09110-6eed-75b6-ba61-86c0a4ba5e46.parquet
#>   data_file_size_bytes data_file_footer_size data_file_encryption_key
#> 1                 2911                  1128                     NULL
#>   delete_file delete_file_size_bytes delete_file_footer_size
#> 1        <NA>                     NA                      NA
#>   delete_file_encryption_key
#> 1                       NULL

detach_ducklake("listfiles_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
