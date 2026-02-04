# Query a table at a specific timestamp (time travel)

Retrieves data from a DuckDB table as it existed at a specific point in
time using DuckDB's snapshot/time-travel functionality.

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

DuckDB supports time-travel queries using the ASOF syntax, allowing you
to query historical data as it existed at a specific timestamp. This is
useful for:

- Auditing changes over time

- Recovering accidentally deleted or modified data

- Comparing data states across different time points

Note: This functionality requires that the table has been properly
configured with DuckDB's time-travel features (e.g., Delta Lake tables
with snapshot support).

## Examples

``` r
if (FALSE) { # \dontrun{
# Query data as it existed yesterday
yesterday <- Sys.time() - (24 * 60 * 60)
get_ducklake_table_asof("my_table", yesterday) |>
  filter(category == "A") |>
  collect()

# Query data at a specific timestamp
get_ducklake_table_asof("my_table", "2024-01-15 10:30:00") |>
  summarise(total = sum(amount))
} # }
```
