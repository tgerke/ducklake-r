# Plot the rows changed in each snapshot of a table

Draws a table's change volume as a diverging bar chart: one bar per
snapshot, with rows inserted or updated above the axis and rows deleted
below it. A companion to
[`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md),
which shows *when* and *what kind* of changes happened; this shows *how
much* changed each time.

## Usage

``` r
plot_table_changes(table_name, ducklake_name = NULL, conn = NULL)
```

## Arguments

- table_name:

  The name of the table to plot.

- ducklake_name:

  The name of the ducklake (database) to query. If NULL, will attempt to
  infer from current database.

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

A ggplot object, which can be further customized with ggplot2 functions

## Details

Requires the ggplot2 package (listed in Suggests). Row counts come from
DuckLake's data change feed via
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md).
An update appears in the feed as a before and an after image of the row;
it is counted once here. Snapshots that touched the table without
changing rows (a schema change, for example) keep their slot on the axis
with no bar.

## See also

Other time travel:
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md),
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md),
[`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md),
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Rows inserted, updated, and deleted per snapshot
plot_table_changes("my_table")

# Customize the result like any ggplot
plot_table_changes("my_table") +
  ggplot2::theme_classic()
} # }
```
