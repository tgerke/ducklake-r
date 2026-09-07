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

## See also

[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Run all maintenance
checkpoint_ducklake()

# Or specify a named lake
checkpoint_ducklake("my_lake")
} # }
```
