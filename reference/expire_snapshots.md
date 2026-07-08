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
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Preview what a one-week retention policy would remove
expire_snapshots(older_than = Sys.time() - 7 * 24 * 60 * 60, dry_run = TRUE)

# Expire it for real, then reclaim the storage
expire_snapshots(older_than = Sys.time() - 7 * 24 * 60 * 60)
cleanup_old_files(cleanup_all = TRUE)

# Expire two specific snapshots
expire_snapshots(versions = c(2, 3))
} # }
```
