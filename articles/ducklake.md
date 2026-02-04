# Getting Started with ducklake

``` r
library(ducklake)
library(dplyr)
```

## Introduction

ducklake is an R package that complements the existing toolkits in the
[duckdb](https://r.duckdb.org/index.html) and
[duckplyr](https://duckplyr.tidyverse.org/index.html) packages,
supporting the new
[DuckLake](https://ducklake.select/docs/stable/duckdb/introduction.html)
ecosystem.

## Installation

Install the development version of ducklake with:

``` r
pak::pak("tgerke/ducklake-r")
```

## Create a local duckdb lakehouse

``` r
# install the ducklake extension to duckdb 
# requires that you already have DuckDB v1.3.0 or higher
install_ducklake()

# create the ducklake
attach_ducklake("my_ducklake")
# show that we have ducklake files
list.files()

# create a table using the Netherlands train traffic dataset 
create_table("nl_train_stations", "https://blobs.duckdb.org/nl_stations.csv")
# show that we now have a .files directory
list.files()
# main/ is where the parquet files go
list.files("my_ducklake.ducklake.files/main/nl_train_stations")

# create a table from an R data.frame
create_table("mtcars_table", mtcars)
list.files("my_ducklake.ducklake.files/main/mtcars_table")
```

## View metadata and snapshots

``` r
# List all tables in the lake
get_ducklake_table("duckdb_tables") |> 
  select(database_name, schema_name, table_name) |> 
  print(n = Inf)

# View snapshot history
get_metadata_table("ducklake_snapshot_changes", ducklake_name = "my_ducklake")
get_metadata_table("ducklake_snapshot", ducklake_name = "my_ducklake")
```

## Cleanup

``` r
# When done, detach from the ducklake
detach_ducklake("my_ducklake")
```

## Next steps

- Learn about [modifying
  tables](https://tgerke.github.io/ducklake-r/articles/modifying-tables.md)
- Explore [upsert
  operations](https://tgerke.github.io/ducklake-r/articles/upsert-operations.md)
- Work with
  [transactions](https://tgerke.github.io/ducklake-r/articles/transactions.md)
- Query historical data with [time
  travel](https://tgerke.github.io/ducklake-r/articles/time-travel.md)
