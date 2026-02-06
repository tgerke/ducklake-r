# Query a table at a specific timestamp (time travel)

Retrieves data from a DuckLake table as it existed at a specific point
in time using DuckLake's AT (TIMESTAMP =\> ...) syntax.

## Usage

``` r
get_ducklake_table_asof(table_name, timestamp, conn = NULL)
```

## Arguments

- table_name:

  The name of the table to query

- timestamp:

  A POSIXct timestamp or character string in ISO 8601 format (e.g.,
  "2024-01-15 10:30:00")

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

A dplyr lazy query object (tbl_lazy) that can be further manipulated
with dplyr verbs

## Details

DuckLake supports time-travel queries, allowing you to query historical
data as it existed at a specific timestamp. This uses the syntax:
`SELECT * FROM table AT (TIMESTAMP => 'timestamp')`

This is useful for:

- Auditing changes over time

- Recovering accidentally deleted or modified data

- Comparing data states across different time points

- Regulatory compliance and data lineage documentation

The timestamp must be within the range of available snapshots for the
table. Use
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md)
to see available snapshot times.

## Examples

``` r
if (FALSE) { # \dontrun{
# Query data as it existed yesterday
yesterday <- Sys.time() - (24 * 60 * 60)
get_ducklake_table_asof("my_table", yesterday) |>
  filter(category == "A") |>
  collect()

# Query data at a specific snapshot time
snapshots <- list_table_snapshots("my_table")
get_ducklake_table_asof("my_table", snapshots$snapshot_time[2]) |>
  summarise(total = sum(amount))
} # }
```
