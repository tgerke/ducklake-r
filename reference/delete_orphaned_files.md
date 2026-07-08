# Delete orphaned files

Deletes files sitting in the lake's data path that are not tracked in
the DuckLake metadata at all – for example, leftovers from a crashed
write.

## Usage

``` r
delete_orphaned_files(
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

This differs from
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
which removes files that *were* tracked but are scheduled for deletion.

Always run with `dry_run = TRUE` first and check the file list. Anything
in the data path that DuckLake does not recognise is fair game, and the
comparison is by exact path string: a data path registered with an
irregularity such as a doubled slash (as R's
[`tempdir()`](https://rdrr.io/r/base/tempfile.html) produces on macOS)
makes *live* files look orphaned, and deleting them breaks the lake.

## See also

[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)

Other maintenance:
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Always preview orphan deletion first
delete_orphaned_files(dry_run = TRUE, cleanup_all = TRUE)
delete_orphaned_files(cleanup_all = TRUE)
} # }
```
