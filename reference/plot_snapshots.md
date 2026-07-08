# Plot the snapshot history of a table or lake

Draws a table's snapshot history as a commit-log style timeline: one row
per snapshot (newest at top), positioned by snapshot time, colored by
the kind of change, and annotated with the author and commit message
where those were recorded (see
[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
and
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)).

## Usage

``` r
plot_snapshots(table_name = NULL, ducklake_name = NULL, conn = NULL)
```

## Arguments

- table_name:

  The name of the table to plot. If NULL, plots all snapshots in the
  ducklake.

- ducklake_name:

  The name of the ducklake (database) to query. If NULL, will attempt to
  infer from current database.

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

A ggplot object, which can be further customized with ggplot2 functions

## Details

Requires the ggplot2 package (listed in Suggests). Snapshot data comes
from
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md);
each snapshot is classified from its `changes` column into one of:
created, schema change, data change, maintenance, or other.

## See also

Other time travel:
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md),
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md),
[`plot_table_changes()`](https://tgerke.github.io/ducklake-r/reference/plot_table_changes.md),
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Plot the snapshot history of a table
plot_snapshots("my_table")

# Plot every snapshot in the lake
plot_snapshots()

# Customize the result like any ggplot
plot_snapshots("my_table") +
  ggplot2::theme_classic()
} # }
```
