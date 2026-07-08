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

As an alternative to calling this manually, a retention policy can be
set once on the catalog with
`DBI::dbExecute(get_ducklake_connection(), "CALL my_lake.set_option('delete_older_than', '1 week')")`,
after which DuckLake cleans up eligible files automatically.

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
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Preview, then delete everything that is scheduled
cleanup_old_files(dry_run = TRUE, cleanup_all = TRUE)
cleanup_old_files(cleanup_all = TRUE)

# Only delete files scheduled more than a week ago
cleanup_old_files(older_than = Sys.time() - 7 * 24 * 60 * 60)
} # }
```
