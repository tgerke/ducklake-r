# Run a DuckLake checkpoint

Runs all maintenance operations on the DuckLake catalog: flushes inlined
data, expires old snapshots, merges small files, and cleans up
unreferenced files.

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

`CHECKPOINT` is the recommended one-stop maintenance command. It
internally calls
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md)
along with compaction, snapshot expiration, and file cleanup.

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
[`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)

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
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Run all maintenance
checkpoint_ducklake()

# Or specify a named lake
checkpoint_ducklake("my_lake")
} # }
```
