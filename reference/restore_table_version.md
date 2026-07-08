# Restore a table to a previous version

Rolls a table back to the state it had at an earlier snapshot or point
in time, by recreating it from a time-travel read of itself. History is
preserved: the restore is recorded as a **new** snapshot (with a commit
message noting the restore), so nothing is rewritten or lost and you can
still time-travel to any snapshot, including those after the restore
point.

## Usage

``` r
restore_table_version(
  table_name,
  version = NULL,
  timestamp = NULL,
  author = NULL,
  commit_message = NULL,
  conn = NULL
)
```

## Arguments

- table_name:

  The name of the table to restore

- version:

  Optional snapshot id to restore to (see
  [`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md))

- timestamp:

  Optional timestamp to restore to (POSIXct or character)

- author:

  Optional author to record on the restore snapshot, for the audit trail

- commit_message:

  Optional commit message for the restore snapshot. Defaults to a
  message noting the restore point (e.g.
  `"Restored my_table to snapshot 5"`).

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

Invisibly returns TRUE on success

## Details

You must specify either `version` or `timestamp`, but not both.

Under the hood this runs
`CREATE OR REPLACE TABLE t AS SELECT * FROM t AT (VERSION => n)` inside
a transaction. Because the restore creates a new snapshot, it is itself
reversible with another `restore_table_version()` call.

## See also

[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md)

Other time travel:
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md),
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md),
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Restore to snapshot 5
restore_table_version("my_table", version = 5)

# Restore to a specific timestamp
restore_table_version("my_table", timestamp = "2024-01-15 10:00:00")

# Record who performed the restore in the audit trail
restore_table_version(
  "my_table",
  version = 5,
  author = "Data Steward",
  commit_message = "Roll back erroneous bulk update"
)
} # }
```
