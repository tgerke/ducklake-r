# Time Travel Queries

``` r
library(ducklake)
library(dplyr)
```

## Introduction to time travel

DuckLake supports querying historical data at specific points in time
using its built-in snapshot functionality. This allows you to:

- View data as it existed at a specific timestamp
- Query specific versions of your tables
- Restore tables to previous states
- Audit changes over time

## Time travel functions

The package provides several time-travel functions:

### Query data at a specific timestamp

``` r
# Query data as it existed at a specific timestamp
get_ducklake_table_asof("my_delta_table", "2024-01-15 10:30:00") |>
  filter(status == "active") |>
  collect()

# Query data as it existed yesterday
yesterday <- Sys.time() - (24 * 60 * 60)
get_ducklake_table_asof("my_delta_table", yesterday) |>
  summarise(n = n())
```

### Query a specific version

``` r
# Query a specific version/snapshot number
get_ducklake_table_version("my_delta_table", version = 5) |>
  collect()
```

### List available snapshots

``` r
# List all available snapshots for a table
list_table_snapshots("my_delta_table")
```

### Restore to a previous version

``` r
# Restore table to a previous version by version number
restore_table_version("my_delta_table", version = 3)

# Or restore to a specific timestamp
restore_table_version("my_delta_table", timestamp = "2024-01-15 10:00:00")
```

## Use cases

Time travel is particularly useful for:

- **Auditing**: Track who changed what and when
- **Debugging**: Identify when data issues were introduced
- **Recovery**: Restore accidentally modified or deleted data
- **Reporting**: Generate reports based on historical data snapshots
- **Compliance**: Maintain historical records for regulatory
  requirements

## Example workflow

``` r
# Setup
install_ducklake()
attach_ducklake("my_ducklake")
create_table(employee_data, "employees")

# Make some changes over time
rows_update(get_ducklake_table("employees"), updates_jan, by = "id")
rows_update(get_ducklake_table("employees"), updates_feb, by = "id")

# View snapshot history
list_table_snapshots("employees")

# Compare current data with previous version
current <- get_ducklake_table("employees") |> collect()
previous <- get_ducklake_table_version("employees", version = 2) |> collect()

# If needed, restore to previous version
restore_table_version("employees", version = 2)
```
