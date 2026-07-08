# Plot the snapshot history of a table or lake

Draws snapshot history in one of two layouts. With a `table_name`, a
commit-log timeline: one row per snapshot (newest at top) on an ordinal
spine, with the timestamp, author, and commit message as aligned text
and long idle stretches marked inline (e.g. "103 days later") instead of
stretching an axis. Without a `table_name`, a lake-wide swimlane: one
row per table, one point per snapshot, evenly spaced in snapshot order,
so active and stale tables read at a glance.

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
created, schema change, data change, maintenance, or other. Authors and
commit messages appear where they were recorded (see
[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
and
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)).

Both layouts position snapshots by order rather than by clock time, so a
history with months of silence between bursts of activity stays
readable. In the swimlane, snapshots that touch no table (like the
initial schema creation) appear in a `(lake)` lane, and the x axis
labels show each snapshot's date.

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
# Commit-log timeline of one table's history
plot_snapshots("my_table")

# Swimlane of every table in the lake
plot_snapshots()

# Customize the result like any ggplot
plot_snapshots("my_table") +
  ggplot2::labs(title = "Audit trail")
} # }
```
