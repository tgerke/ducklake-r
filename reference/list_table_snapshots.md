# List available snapshots for a table

Retrieves information about available snapshots/versions for a table.

## Usage

``` r
list_table_snapshots(table_name, conn = NULL)
```

## Arguments

- table_name:

  The name of the table to query

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

A data frame with snapshot information (version, timestamp, etc.)

## Details

This function queries the snapshot history of a table, showing available
versions and their timestamps. This is useful for understanding what
historical versions are available for time-travel queries.

Note: The exact format and availability of this information depends on
the table format (Delta Lake, Iceberg, etc.).

## Examples

``` r
if (FALSE) { # \dontrun{
# List all snapshots for a table
list_table_snapshots("my_table")
} # }
```
