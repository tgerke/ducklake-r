# Flush inlined data to Parquet files

Materialises data that has been stored inline in the catalog database
into Parquet files on the data path. This includes both inlined inserts
and inlined deletions.

## Usage

``` r
flush_inlined_data(ducklake_name = NULL, table_name = NULL, schema_name = NULL)
```

## Arguments

- ducklake_name:

  Name of the attached DuckLake catalog. If `NULL`, the current database
  is used.

- table_name:

  Optional table name. When provided, only flushes inlined data for that
  table.

- schema_name:

  Optional schema name. When provided, only flushes inlined data for
  tables in that schema.

## Value

A data frame with columns `schema_name`, `table_name`, and
`rows_flushed`. Tables with no inlined data are omitted.

## Details

Flushing writes inlined rows to consolidated Parquet files and cleans up
the inlined data tables. Time-travel information is preserved: flushed
rows that had been deleted will produce a partial deletion file with
snapshot metadata.

Tables with `auto_compact` set to `FALSE` are skipped when flushing an
entire lake or schema. Use an explicit `table_name` to flush those
tables.

If a table has a sort order defined, the flushed Parquet file will be
sorted by those keys.

## See also

[`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)

Other data inlining:
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`get_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/get_inlining_row_limit.md),
[`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)

Other maintenance:
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md),
[`list_ducklake_files()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_files.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Flush everything
flush_inlined_data()

# Flush a specific table
flush_inlined_data(table_name = "readings")

# Flush a specific schema
flush_inlined_data(schema_name = "staging")
} # }
```
