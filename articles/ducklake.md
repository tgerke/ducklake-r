# ducklake Cookbook

``` r

library(ducklake)
library(dplyr)
```

## Introduction

This cookbook provides quick recipes for common ducklake operations.
Each recipe is a self-contained example you can adapt for your workflow.

For a comprehensive real-world example, see the [clinical trial data
lake](https://tgerke.github.io/ducklake-r/articles/clinical-trial-datalake.md)
vignette.

## Setup recipes

### Create a new data lake

``` r

# Create a data lake in a specific directory
attach_ducklake("my_lake", lake_path = vignette_temp_dir)
```

### Attach to an existing data lake

``` r

# Attach to an existing lake (creates it if it doesn't exist)
attach_ducklake("existing_lake", lake_path = "/path/to/data_lake")
```

### Use an alternative catalog backend

``` r

# PostgreSQL catalog for multi-client access
attach_ducklake(
  "shared_lake",
  backend = "postgres",
  catalog_connection_string = "dbname=ducklake_catalog host=localhost",
  lake_path = "/shared/lake/data/"
)

# SQLite catalog for lightweight local multi-client setups
attach_ducklake(
  "team_lake",
  backend = "sqlite",
  catalog_connection_string = "metadata.sqlite",
  lake_path = "data_files/"
)
```

### Detach from a data lake

``` r

# Detach when done (doesn't delete the lake)
detach_ducklake("my_lake")
```

## Loading data recipes

### Load data from a data.frame

``` r

with_transaction(
  create_table(mtcars, "cars"),
  author = "Data Engineer",
  commit_message = "Initial car data load"
)
#> Transaction started.
#> Transaction committed.
```

### Update an existing table

``` r

# Create a second version of the cars table
with_transaction(
  get_ducklake_table("cars") |>
    mutate(kpl = mpg * 0.425144) |>  # Add km/L conversion
    replace_table("cars"),
  author = "Data Engineer",
  commit_message = "Add km/L metric to cars table"
)
#> Transaction started.
#> Transaction committed.
```

### Load data from a CSV file

``` r

# First write a sample CSV (in practice, you'd have an existing file)
csv_path <- file.path(vignette_temp_dir, "sample_data.csv")
write.csv(head(iris, 20), csv_path, row.names = FALSE)

# Load the CSV into the data lake
with_transaction(
  create_table(csv_path, "iris_sample"),
  author = "Data Engineer",
  commit_message = "Load iris sample from CSV"
)
#> Transaction started.
#> Transaction committed.
```

### Load data from a URL

``` r

# ducklake can load data directly from URLs
with_transaction(
  create_table("https://example.com/data.csv", "remote_data"),
  author = "Data Engineer",
  commit_message = "Load remote dataset"
)
```

### Load with a dplyr pipeline

``` r

with_transaction(
  mtcars |>
    filter(mpg > 20) |>
    create_table("efficient_cars"),
  author = "Data Analyst",
  commit_message = "Load filtered car data"
)
#> Transaction started.
#> Transaction committed.
```

### List all tables in the lake

``` r

# See what tables exist in your lake
get_ducklake_table("duckdb_tables") |>
  filter(schema_name == "main") |>
  select(table_name) |>
  collect() |>
  print(n = Inf)
#> # A tibble: 3 × 1
#>   table_name    
#>   <chr>         
#> 1 efficient_cars
#> 2 iris_sample   
#> 3 cars
```

## Reading data recipes

### Read a table

``` r

# Returns a lazy dplyr tbl
cars_data <- get_ducklake_table("cars")

# Use dplyr verbs
cars_data |>
  filter(cyl == 6) |>
  select(mpg, cyl, hp) |>
  head(3)
#> # A query:  ?? x 3
#> # Database: DuckDB 1.5.4 [unknown@Linux 6.17.0-1018-azure:R 4.6.1//tmp/Rtmpy2Rhbt/ducklake/ducklake1fc3284ed000.duckdb]
#>     mpg   cyl    hp
#>   <dbl> <dbl> <dbl>
#> 1  21       6   110
#> 2  21       6   110
#> 3  21.4     6   110
```

### Collect data into memory

``` r

# Fetch all data into a data.frame
cars_df <- get_ducklake_table("cars") |> collect()
head(cars_df, 3)
#> # A tibble: 3 × 12
#>     mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb   kpl
#>   <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1  21       6   160   110  3.9   2.62  16.5     0     1     4     4  8.93
#> 2  21       6   160   110  3.9   2.88  17.0     0     1     4     4  8.93
#> 3  22.8     4   108    93  3.85  2.32  18.6     1     1     4     1  9.69
```

### View all versions of a table

``` r

# See all snapshots for the cars table
list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-07-08 04:39:47              1
#> 2           2 2026-07-08 04:39:48              2
#>                                                                 changes
#> 1                    tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#>          author                commit_message commit_extra_info
#> 1 Data Engineer         Initial car data load              <NA>
#> 2 Data Engineer Add km/L metric to cars table              <NA>
```

### Read a specific version

``` r

# Query data as it existed at snapshot 1 -- before the kpl column was added
get_ducklake_table_version("cars", version = 1) |>
  select(mpg, cyl, hp) |>
  head(3)
#> # A query:  ?? x 3
#> # Database: DuckDB 1.5.4 [unknown@Linux 6.17.0-1018-azure:R 4.6.1//tmp/Rtmpy2Rhbt/ducklake/ducklake1fc3284ed000.duckdb]
#>     mpg   cyl    hp
#>   <dbl> <dbl> <dbl>
#> 1  21       6   110
#> 2  21       6   110
#> 3  22.8     4    93
```

### Read data at a specific timestamp

``` r

# Query data as of a specific time (see list_table_snapshots() for times)
get_ducklake_table_asof("cars", timestamp = "2024-01-15 10:30:00") |>
  collect()
```

## Updating data recipes

### Replace entire table

``` r

with_transaction(
  get_ducklake_table("cars") |>
    mutate(hp_per_cyl = hp / as.numeric(cyl)) |>  # Add derived metric
    replace_table("cars"),
  author = "Data Engineer",
  commit_message = "Add horsepower per cylinder metric"
)
#> Transaction started.
#> Transaction committed.
```

Note: Use
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
for structural changes (adding or removing columns) and the row-level
operations
([`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
[`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
[`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md))
for targeted, incremental changes. Both are fully versioned – every
committed change creates a snapshot you can time-travel back to. See
[`vignette("modifying-tables")`](https://tgerke.github.io/ducklake-r/articles/modifying-tables.md)
for guidance on choosing between them.

## Metadata and versioning recipes

### View all snapshots

``` r

list_table_snapshots()
#>   snapshot_id       snapshot_time schema_version
#> 1           0 2026-07-08 04:39:47              0
#> 2           1 2026-07-08 04:39:47              1
#> 3           2 2026-07-08 04:39:48              2
#> 4           3 2026-07-08 04:39:48              3
#> 5           4 2026-07-08 04:39:48              4
#> 6           5 2026-07-08 04:39:49              5
#>                                                                 changes
#> 1                                                 schemas_created, main
#> 2                    tables_created, tables_inserted_into, main.cars, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 4             tables_created, tables_inserted_into, main.iris_sample, 3
#> 5          tables_created, tables_inserted_into, main.efficient_cars, 4
#> 6 tables_created, tables_dropped, tables_inserted_into, main.cars, 2, 5
#>          author                     commit_message commit_extra_info
#> 1          <NA>                               <NA>              <NA>
#> 2 Data Engineer              Initial car data load              <NA>
#> 3 Data Engineer      Add km/L metric to cars table              <NA>
#> 4 Data Engineer          Load iris sample from CSV              <NA>
#> 5  Data Analyst             Load filtered car data              <NA>
#> 6 Data Engineer Add horsepower per cylinder metric              <NA>
```

### View snapshots for a specific table

``` r

list_table_snapshots("cars")
```

### Restore a table to a previous version

``` r

# Roll cars back to snapshot 1. The restore is recorded as a new snapshot,
# so nothing is lost -- you can still time-travel to any version.
restore_table_version(
  "cars",
  version = 1,
  author = "Data Engineer"
)
#> Transaction started.
#> Transaction committed.
#> Table "cars" restored to snapshot 1 (recorded as a new snapshot).

list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-07-08 04:39:47              1
#> 2           2 2026-07-08 04:39:48              2
#> 3           5 2026-07-08 04:39:49              5
#> 4           6 2026-07-08 04:39:49              6
#>                                                                 changes
#> 1                    tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 2, 5
#> 4 tables_created, tables_dropped, tables_inserted_into, main.cars, 5, 6
#>          author                     commit_message commit_extra_info
#> 1 Data Engineer              Initial car data load              <NA>
#> 2 Data Engineer      Add km/L metric to cars table              <NA>
#> 3 Data Engineer Add horsepower per cylinder metric              <NA>
#> 4 Data Engineer        Restored cars to snapshot 1              <NA>
```

## Transaction recipes

### Simple transaction

``` r

with_transaction(
  create_table(my_data, "my_table"),
  author = "Your Name",
  commit_message = "What changed and why"
)
```

### Multi-step transaction

``` r

with_transaction({
  # All these operations happen atomically
  create_table(raw_data, "raw_table")
  
  cleaned <- get_ducklake_table("raw_table") |>
    filter(!is.na(key_field)) |>
    create_table("clean_table")
  
  get_ducklake_table("clean_table") |>
    mutate(derived_field = calculate_something(x)) |>
    create_table("analysis_table")
},
author = "Data Engineer",
commit_message = "Full ETL pipeline run"
)
```

### Manual transaction control

``` r

# For fine-grained control
begin_transaction()

create_table(data1, "table1")
create_table(data2, "table2")

# Commit or rollback
commit_transaction(
  author = "Your Name",
  commit_message = "Manual transaction commit"
)

# Or if something went wrong:
# rollback_transaction()
```

## Query optimization recipes

### Preview query without execution

To see the SQL a *read* pipeline will run, use dplyr’s
[`show_query()`](https://dplyr.tidyverse.org/reference/explain.html):

``` r

get_ducklake_table("cars") |>
  filter(mpg > 25) |>
  select(mpg, cyl, hp) |>
  show_query()
#> <SQL>
#> SELECT mpg, cyl, hp
#> FROM cars
#> WHERE (mpg > 25.0)
```

To preview the SQL an in-place *modification* would run (before
committing to it with
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)),
use
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md):

``` r

get_ducklake_table("cars") |>
  mutate(mpg = round(mpg)) |>
  show_ducklake_query()
#> 
#> === DuckLake SQL Preview ===
#> 
#> -- Main operation
#> UPDATE cars SET mpg = ROUND_EVEN(mpg, CAST(ROUND(0.0, 0) AS INTEGER)) ;
```

### Filter early for performance

``` r

# Good: Filter before other operations
get_ducklake_table("cars") |>
  filter(cyl == 6) |>
  mutate(kpl = mpg * 0.425144) |>
  head(3)
#> # A query:  ?? x 12
#> # Database: DuckDB 1.5.4 [unknown@Linux 6.17.0-1018-azure:R 4.6.1//tmp/Rtmpy2Rhbt/ducklake/ducklake1fc3284ed000.duckdb]
#>     mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb   kpl
#>   <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1  21       6   160   110  3.9   2.62  16.5     0     1     4     4  8.93
#> 2  21       6   160   110  3.9   2.88  17.0     0     1     4     4  8.93
#> 3  21.4     6   258   110  3.08  3.22  19.4     1     0     3     1  9.10
```

### Use specific columns

``` r

# Good: Select only needed columns
get_ducklake_table("cars") |>
  select(mpg, cyl, hp) |>
  filter(mpg > 25)
#> # A query:  ?? x 3
#> # Database: DuckDB 1.5.4 [unknown@Linux 6.17.0-1018-azure:R 4.6.1//tmp/Rtmpy2Rhbt/ducklake/ducklake1fc3284ed000.duckdb]
#>     mpg   cyl    hp
#>   <dbl> <dbl> <dbl>
#> 1  32.4     4    66
#> 2  30.4     4    52
#> 3  33.9     4    65
#> 4  27.3     4    66
#> 5  26       4    91
#> 6  30.4     4   113
```

## Cleanup

``` r

# Detach from the lake
detach_ducklake("my_lake")
```

## See also

- [Modifying
  Tables](https://tgerke.github.io/ducklake-r/articles/modifying-tables.md) -
  Detailed guide to table modification approaches
- [Transactions](https://tgerke.github.io/ducklake-r/articles/transactions.md) -
  Advanced transaction patterns
- [Time
  Travel](https://tgerke.github.io/ducklake-r/articles/time-travel.md) -
  Comprehensive time travel guide
- [Clinical Trial Data
  Lake](https://tgerke.github.io/ducklake-r/articles/clinical-trial-datalake.md) -
  Complete real-world workflow
