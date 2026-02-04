# Restore a table to a previous version

Restores a table to a specific version or timestamp, reverting any
changes made after that point.

## Usage

``` r
restore_table_version(
  table_name,
  version = NULL,
  timestamp = NULL,
  conn = NULL
)
```

## Arguments

- table_name:

  The name of the table to restore

- version:

  Optional version number to restore to

- timestamp:

  Optional timestamp to restore to (POSIXct or character)

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

Invisibly returns TRUE on success

## Details

This function restores a table to a previous state. You must specify
either `version` or `timestamp`, but not both.

WARNING: This operation modifies the table and cannot be easily undone.
Consider using within a transaction or backing up your data first.

## Examples

``` r
if (FALSE) { # \dontrun{
# Restore to version 5
restore_table_version("my_table", version = 5)

# Restore to a specific timestamp
restore_table_version("my_table", timestamp = "2024-01-15 10:00:00")
} # }
```
