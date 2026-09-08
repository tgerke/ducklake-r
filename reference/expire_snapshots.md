# Expire old snapshots

Removes old snapshots from the DuckLake catalog. Expiring snapshots
gives up the ability to time-travel to them, and schedules the data
files that only they referenced for deletion.

## Usage

``` r
expire_snapshots(
  ducklake_name = NULL,
  older_than = NULL,
  versions = NULL,
  dry_run = FALSE
)
```

## Arguments

- ducklake_name:

  Name of the attached DuckLake catalog. If `NULL`, the current database
  is used.

- older_than:

  Expire all snapshots older than this timestamp (POSIXct or character
  in ISO 8601 format). POSIXct values are converted to UTC, which is how
  DuckLake records snapshot times; character values must already be UTC.
  At least one of `older_than` or `versions` must be provided.

- versions:

  Integer vector of specific snapshot ids to expire (see
  [`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md)).

- dry_run:

  If `TRUE`, only lists the snapshots that would be expired without
  expiring them.

## Value

A data frame listing the expired (or, with `dry_run = TRUE`, expirable)
snapshots.

## Details

Expiring snapshots does not delete any files by itself: files that are
no longer referenced are merely scheduled for deletion. Run
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md)
afterwards to reclaim the storage, or let
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)
handle both steps.

The most recent snapshot can never be expired.

For a standing policy, set
`set_ducklake_option("expire_older_than", "90 days")` once;
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)
then expires eligible snapshots on every run. Without that option a
checkpoint expires nothing.

## See also

[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md)

Other maintenance:
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md),
[`list_ducklake_files()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_files.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
lake_dir <- tempfile("expire_lake_")
dir.create(lake_dir)
attach_ducklake("expire_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

rows_insert(
  get_ducklake_table("cars"),
  data.frame(mpg = 30, cyl = 4),
  by = "mpg"
)

# Preview what a one-week retention policy would remove
expire_snapshots(older_than = Sys.time() - 7 * 24 * 60 * 60, dry_run = TRUE)
#> Dry run: 0 snapshots would be expired.
#> [1] snapshot_id       snapshot_time     schema_version    changes          
#> [5] author            commit_message    commit_extra_info
#> <0 rows> (or 0-length row.names)

# Expire everything older than now, then reclaim the storage
expire_snapshots(older_than = Sys.time())
#> Expired 2 snapshots.
#> ℹ Unreferenced files are scheduled for deletion; run `cleanup_old_files()` to
#>   reclaim storage.
#>   snapshot_id       snapshot_time schema_version
#> 1           0 2026-09-08 15:12:57              0
#> 2           1 2026-09-08 15:12:57              1
#>                                              changes author commit_message
#> 1                              schemas_created, main   <NA>           <NA>
#> 2 tables_created, tables_inserted_into, main.cars, 1   <NA>           <NA>
#>   commit_extra_info
#> 1              <NA>
#> 2              <NA>
cleanup_old_files(cleanup_all = TRUE)
#> Deleted 0 old file.
#> [1] path
#> <0 rows> (or 0-length row.names)

detach_ducklake("expire_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
