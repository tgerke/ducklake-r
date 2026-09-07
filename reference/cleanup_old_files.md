# Delete files scheduled for removal

Physically deletes data files that are no longer referenced by any
snapshot – typically files orphaned by
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md)
or replaced by
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md).

## Usage

``` r
cleanup_old_files(
  ducklake_name = NULL,
  older_than = NULL,
  cleanup_all = FALSE,
  dry_run = FALSE
)
```

## Arguments

- ducklake_name:

  Name of the attached DuckLake catalog. If `NULL`, the current database
  is used.

- older_than:

  Only delete files scheduled for deletion before this timestamp
  (POSIXct, converted to UTC, or character already in UTC). One of
  `older_than` or `cleanup_all` is required.

- cleanup_all:

  If `TRUE`, delete all scheduled files regardless of when they were
  scheduled.

- dry_run:

  If `TRUE`, only lists the files that would be deleted.

## Value

A data frame listing the deleted (or deletable) files.

## Details

As an alternative to calling this manually, set a retention policy once
with `set_ducklake_option("delete_older_than", "7 days")`; every later
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)
then deletes the files that have been released for at least that long.

## See also

[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)

Other maintenance:
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md),
[`list_ducklake_files()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_files.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
lake_dir <- tempfile("cleanup_lake_")
dir.create(lake_dir)
attach_ducklake("cleanup_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

expire_snapshots(older_than = Sys.time())
#> Expired 1 snapshot.
#> ℹ Unreferenced files are scheduled for deletion; run `cleanup_old_files()` to
#>   reclaim storage.
#>   snapshot_id       snapshot_time schema_version               changes author
#> 1           0 2026-09-07 17:10:56              0 schemas_created, main   <NA>
#>   commit_message commit_extra_info
#> 1           <NA>              <NA>

# Preview, then delete everything that is scheduled
cleanup_old_files(dry_run = TRUE, cleanup_all = TRUE)
#> Dry run: 0 old file would be deleted.
#> [1] path
#> <0 rows> (or 0-length row.names)
cleanup_old_files(cleanup_all = TRUE)
#> Deleted 0 old file.
#> [1] path
#> <0 rows> (or 0-length row.names)

# Only delete files scheduled more than a week ago
cleanup_old_files(older_than = Sys.time() - 7 * 24 * 60 * 60)
#> Deleted 0 old file.
#> [1] path
#> <0 rows> (or 0-length row.names)

detach_ducklake("cleanup_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
