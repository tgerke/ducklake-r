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

### Register existing Parquet files without copying

If your data is already in Parquet,
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md)
records the files in the lake in place – no copy, no rewrite, and no
collection into R. A vector of files is registered atomically in one
snapshot. This is the fast migration path from a folder of Parquet
extracts. The target table can already exist with a compatible schema,
or `create = TRUE` can create it directly from the Parquet schema. Note
that the lake takes ownership of the files: later compaction may rewrite
or delete them.

``` r

add_data_files(
  "readings",
  c("extracts/jan.parquet", "extracts/feb.parquet"),
  create = TRUE
)

# See which files back a table
list_ducklake_files("readings")
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

# Every table and view, with its schema and type
list_ducklake_tables()
#>   schema_name     table_name  type
#> 1        main           cars table
#> 2        main efficient_cars table
#> 3        main    iris_sample table
```

## Shared logic and documentation recipes

### Store a pipeline as a view

A view stores a query, not data: reads always run against the current
tables, and every client of the lake – R, Python, or plain SQL – sees
the same definition.

``` r

get_ducklake_table("cars") |>
  filter(mpg > 25) |>
  create_view("v_efficient_cars")
#> Created view "v_efficient_cars".

get_ducklake_table("v_efficient_cars") |> collect()
#> # A tibble: 6 × 12
#>     mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb   kpl
#>   <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1  32.4     4  78.7    66  4.08  2.2   19.5     1     1     4     1  13.8
#> 2  30.4     4  75.7    52  4.93  1.62  18.5     1     1     4     2  12.9
#> 3  33.9     4  71.1    65  4.22  1.84  19.9     1     1     4     1  14.4
#> 4  27.3     4  79      66  4.08  1.94  18.9     1     1     4     1  11.6
#> 5  26       4 120.     91  4.43  2.14  16.7     0     1     5     2  11.1
#> 6  30.4     4  95.1   113  3.77  1.51  16.9     1     1     5     2  12.9
```

Drop it when the logic is no longer needed:

``` r

drop_view("v_efficient_cars")
#> Dropped view "v_efficient_cars".
```

For logic a view cannot hold – a parameterized SQL macro, say – DuckDB
SQL is the escape hatch:
`DBI::dbExecute(get_ducklake_connection(), "CREATE MACRO ...")`.

### Document tables and columns

Comments live in the lake’s catalog, so the documentation travels with
the data instead of in a sidecar file:

``` r

set_table_comment("cars", "Motor Trend road tests of 1973-74 models")
#> Commented table "cars".
set_column_comments(
  "cars",
  mpg = "Miles per US gallon",
  wt = "Weight (1000 lbs)"
)
#> Commented 2 columns on "cars".

get_table_comments("cars")
#>   object_type table_name column_name                                  comment
#> 1      column       cars         mpg                      Miles per US gallon
#> 2      column       cars          wt                        Weight (1000 lbs)
#> 3       table       cars        <NA> Motor Trend road tests of 1973-74 models
```

### Keep variable labels through the lake

If your data carries haven/labelled-style variable labels, they survive
the lake:
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
stores `label` attributes as column comments, and
[`collect()`](https://dplyr.tidyverse.org/reference/compute.html) puts
them back, so label-aware tools like gtsummary and gt behave as if the
data never left R.

``` r

df_visits <- data.frame(subject = c("S1", "S2"), sbp = c(128, 141))
attr(df_visits$subject, "label") <- "Subject identifier"
attr(df_visits$sbp, "label") <- "Systolic blood pressure (mmHg)"

create_table(df_visits, "visits")
#> Stored 2 column labels as column comments.

collected <- get_ducklake_table("visits") |> collect()
attr(collected$sbp, "label")
#> [1] "Systolic blood pressure (mmHg)"
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
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1020-azure:R 4.6.1//tmp/RtmpkONK6z/ducklake/ducklake22204229de8c.duckdb]
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
#> 1           1 2026-08-10 19:08:55              1
#> 2           2 2026-08-10 19:08:55              2
#> 3           7 2026-08-10 19:08:56              7
#> 4           8 2026-08-10 19:08:57              8
#>                                                                 changes
#> 1                    tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 3                                                     tables_altered, 2
#> 4                                                     tables_altered, 2
#>          author                commit_message commit_extra_info
#> 1 Data Engineer         Initial car data load              <NA>
#> 2 Data Engineer Add km/L metric to cars table              <NA>
#> 3          <NA>                          <NA>              <NA>
#> 4          <NA>                          <NA>              <NA>
```

### Read a specific version

``` r

# Query data as it existed at snapshot 1 -- before the kpl column was added
get_ducklake_table_version("cars", version = 1) |>
  select(mpg, cyl, hp) |>
  head(3)
#> # A query:  ?? x 3
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1020-azure:R 4.6.1//tmp/RtmpkONK6z/ducklake/ducklake22204229de8c.duckdb]
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
#> Stored 2 column labels as column comments.
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
#>    snapshot_id       snapshot_time schema_version
#> 1            0 2026-08-10 19:08:55              0
#> 2            1 2026-08-10 19:08:55              1
#> 3            2 2026-08-10 19:08:55              2
#> 4            3 2026-08-10 19:08:56              3
#> 5            4 2026-08-10 19:08:56              4
#> 6            5 2026-08-10 19:08:56              5
#> 7            6 2026-08-10 19:08:56              6
#> 8            7 2026-08-10 19:08:56              7
#> 9            8 2026-08-10 19:08:57              8
#> 10           9 2026-08-10 19:08:57              9
#> 11          10 2026-08-10 19:08:57             10
#>                                                                                     changes
#> 1                                                                     schemas_created, main
#> 2                                        tables_created, tables_inserted_into, main.cars, 1
#> 3                     tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 4                                 tables_created, tables_inserted_into, main.iris_sample, 3
#> 5                              tables_created, tables_inserted_into, main.efficient_cars, 4
#> 6                                                      views_created, main.v_efficient_cars
#> 7                                                                          views_dropped, 5
#> 8                                                                         tables_altered, 2
#> 9                                                                         tables_altered, 2
#> 10                        tables_created, tables_altered, inlined_insert, main.visits, 6, 6
#> 11 tables_created, tables_dropped, tables_altered, tables_inserted_into, main.cars, 2, 7, 7
#>           author                     commit_message commit_extra_info
#> 1           <NA>                               <NA>              <NA>
#> 2  Data Engineer              Initial car data load              <NA>
#> 3  Data Engineer      Add km/L metric to cars table              <NA>
#> 4  Data Engineer          Load iris sample from CSV              <NA>
#> 5   Data Analyst             Load filtered car data              <NA>
#> 6           <NA>                               <NA>              <NA>
#> 7           <NA>                               <NA>              <NA>
#> 8           <NA>                               <NA>              <NA>
#> 9           <NA>                               <NA>              <NA>
#> 10          <NA>                               <NA>              <NA>
#> 11 Data Engineer Add horsepower per cylinder metric              <NA>
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
#> 1           1 2026-08-10 19:08:55              1
#> 2           2 2026-08-10 19:08:55              2
#> 3           7 2026-08-10 19:08:56              7
#> 4           8 2026-08-10 19:08:57              8
#> 5          10 2026-08-10 19:08:57             10
#> 6          11 2026-08-10 19:08:58             11
#>                                                                                    changes
#> 1                                       tables_created, tables_inserted_into, main.cars, 1
#> 2                    tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 3                                                                        tables_altered, 2
#> 4                                                                        tables_altered, 2
#> 5 tables_created, tables_dropped, tables_altered, tables_inserted_into, main.cars, 2, 7, 7
#> 6                    tables_created, tables_dropped, tables_inserted_into, main.cars, 7, 8
#>          author                     commit_message commit_extra_info
#> 1 Data Engineer              Initial car data load              <NA>
#> 2 Data Engineer      Add km/L metric to cars table              <NA>
#> 3          <NA>                               <NA>              <NA>
#> 4          <NA>                               <NA>              <NA>
#> 5 Data Engineer Add horsepower per cylinder metric              <NA>
#> 6 Data Engineer        Restored cars to snapshot 1              <NA>
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
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1020-azure:R 4.6.1//tmp/RtmpkONK6z/ducklake/ducklake22204229de8c.duckdb]
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
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1020-azure:R 4.6.1//tmp/RtmpkONK6z/ducklake/ducklake22204229de8c.duckdb]
#>     mpg   cyl    hp
#>   <dbl> <dbl> <dbl>
#> 1  32.4     4    66
#> 2  30.4     4    52
#> 3  33.9     4    65
#> 4  27.3     4    66
#> 5  26       4    91
#> 6  30.4     4   113
```

### Sort or partition large tables for file pruning

For big tables, declaring a sort order or partition keys lets DuckLake
skip whole Parquet files when a query filters on those columns:

``` r

# Sorting suits high-cardinality columns like timestamps or ids
set_table_sorting("events", "event_time")

# Partitioning suits low-cardinality columns like year or region
set_table_partitioning("sales", c("year(order_date)", "region"))
```

### Tune lake options

[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md)
adjusts DuckLake’s persisted settings at lake, schema, or table scope,
and
[`get_ducklake_options()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_options.md)
shows what’s set:

``` r

# Trade write speed for smaller files
set_ducklake_option("parquet_compression", "zstd")

# Require a commit message on every snapshot -- useful for audit discipline
set_ducklake_option("require_commit_message", TRUE)

get_ducklake_options()
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
