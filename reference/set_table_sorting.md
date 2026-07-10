# Set the sort order of a table

Declares how a table's data files should be sorted. DuckLake sorts data
on insert (unless the `sort_on_insert` option is disabled), during
compaction with
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
and when flushing inlined data with
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md).
Sorted files carry tighter min/max statistics, so filters on the sort
columns prune files instead of scanning them – the complement to
[`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md)
for high-cardinality columns.

## Usage

``` r
set_table_sorting(table_name, sort_by)
```

## Arguments

- table_name:

  The name of the table to sort.

- sort_by:

  Character vector of sort keys. Each entry is a column name, optionally
  followed by `ASC` or `DESC` and by `NULLS FIRST` or `NULLS LAST`, e.g.
  `"event_time DESC"` or `"id ASC NULLS LAST"`.

## Value

Invisibly returns `NULL`.

## Details

Runs `ALTER TABLE ... SET SORTED BY (...)`. Only newly written files are
sorted; existing files keep their layout until compaction rewrites them.

DuckLake also accepts arbitrary SQL expressions as sort keys; this
wrapper deliberately accepts only column-based keys so the input can be
validated. For expression keys, run the `ALTER TABLE` statement directly
with
[`DBI::dbExecute()`](https://dbi.r-dbi.org/reference/dbExecute.html).

To keep insert speed and sort the files only at compaction time, disable
sorting on insert with
`set_ducklake_option("sort_on_insert", FALSE, table_name = ...)`.

## See also

[`reset_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/reset_table_sorting.md),
[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md),
[`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md)

Other sorting:
[`reset_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/reset_table_sorting.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Order events by time so time-window filters prune files
set_table_sorting("events", "event_time")

# Compound key with explicit directions
set_table_sorting("events", c("event_time ASC", "event_type DESC"))
} # }
```
