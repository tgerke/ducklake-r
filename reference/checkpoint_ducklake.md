# Run a DuckLake checkpoint

Runs DuckLake's maintenance operations on the catalog in one call:
flushes inlined data, merges small files, rewrites heavily deleted
files, and, when the lake carries a retention policy, expires old
snapshots and deletes the files they released.

## Usage

``` r
checkpoint_ducklake(ducklake_name = NULL)
```

## Arguments

- ducklake_name:

  Name of the attached DuckLake catalog. If `NULL`, the current database
  is used.

## Value

Invisibly returns `NULL`.

## Details

`CHECKPOINT` runs, in order, the equivalents of
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
and
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md).
The two retention steps do nothing until the lake carries a policy:
snapshots are expired only when the `expire_older_than` option is set,
and released files are deleted only when `delete_older_than` is set. A
lake with neither keeps every snapshot and every file however often it
is checkpointed.

A typical policy, set once with
[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md)
and persisted in the catalog:

    set_ducklake_option("expire_older_than", "90 days")
    set_ducklake_option("delete_older_than", "7 days")

Run checkpoints periodically (e.g., after a batch of streaming inserts)
to consolidate inlined data and keep query performance optimal.

## Note

On Windows with a DuckDB-file catalog, the file-cleanup step of
`CHECKPOINT` can fail because Windows does not allow the catalog file to
be opened a second time while the lake is attached (a current DuckDB
limitation).
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md)
is unaffected; on Windows, prefer it for routine use and run full
checkpoints from a fresh session, or use a PostgreSQL/SQLite catalog.

## See also

[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md)

Other data inlining:
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`get_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/get_inlining_row_limit.md),
[`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)

Other maintenance:
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
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
lake_dir <- tempfile("checkpoint_lake_")
dir.create(lake_dir)
attach_ducklake("checkpoint_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

# Run all maintenance
checkpoint_ducklake()
#> Checkpoint completed for "checkpoint_lake".

# Or specify a named lake
checkpoint_ducklake("checkpoint_lake")
#> Checkpoint completed for "checkpoint_lake".

detach_ducklake("checkpoint_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
