# Plot the file layout of a lake

Draws each table's storage footprint as a horizontal bar: total bytes of
Parquet data files, with delete files stacked in a second color, and a
label giving the file count and average file size. Useful for spotting
fragmentation – many small files – before it slows scans down.

## Usage

``` r
plot_table_files(ducklake_name = NULL, conn = NULL)
```

## Arguments

- ducklake_name:

  The name of the ducklake (database) to query. If NULL, will attempt to
  infer from current database.

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

A ggplot object, which can be further customized with ggplot2 functions

## Details

Requires the ggplot2 package (listed in Suggests). File statistics come
from
[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md).
Tables whose rows are still inlined in the catalog have no data files
yet and show an empty bar; run
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md)
to write them out. Many small files can be compacted with
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
and a large delete-file share is a sign to run
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md).

## See also

Other maintenance:
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# File counts and sizes for every table in the lake
plot_table_files()

# Customize the result like any ggplot
plot_table_files() +
  ggplot2::theme_classic()
} # }
```
