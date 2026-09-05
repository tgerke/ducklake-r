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

### Add a derived column in place

``` r

# A new column is a metadata change; filling it is an in-database UPDATE.
# Together they make the second version of the cars table.
with_transaction({
  add_table_column("cars", "kpl", "DOUBLE")
  get_ducklake_table("cars") |>
    mutate(kpl = mpg * 0.425144) |>  # km/L conversion
    ducklake_exec()
},
  author = "Data Engineer",
  commit_message = "Add km/L metric to cars table"
)
#> Transaction started.
#> Added column "kpl" (DOUBLE) to "cars".
#> ℹ Metadata-only change; no data files were rewritten.
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

### Derive a table from another table, inside the database

A pipeline built on
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md)
is written with `CREATE TABLE ... AS` in DuckDB: the rows never pass
through R, and column labels follow the columns into the new table.

``` r

with_transaction(
  get_ducklake_table("cars") |>
    filter(cyl == 4) |>
    select(mpg, cyl, hp, wt) |>
    create_table("small_cars"),
  author = "Data Analyst",
  commit_message = "Four-cylinder subset"
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
#> 4        main     small_cars table
```

### Organize tables in schemas

Schemas group tables inside the lake. They suit medallion layers
(`bronze`, `silver`, `gold`) or one area per study, and every function
that takes a table name accepts `"schema.table"`:

``` r

create_schema("staging")
#> Created schema "staging".
create_table(mtcars, "staging.cars_raw")

get_ducklake_table("staging.cars_raw") |>
  count(cyl)
#> # A query:  ?? x 2
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpQXKWQP/ducklake/ducklake2b6b63925f89.duckdb]
#>     cyl     n
#>   <dbl> <dbl>
#> 1     6     7
#> 2     8    14
#> 3     4    11

list_ducklake_tables()
#>   schema_name     table_name  type
#> 1        main           cars table
#> 2        main efficient_cars table
#> 3        main    iris_sample table
#> 4        main     small_cars table
#> 5     staging       cars_raw table
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
#>   object_type schema_name table_name column_name
#> 1      column        main       cars         mpg
#> 2      column        main       cars          wt
#> 3       table        main       cars        <NA>
#>                                    comment
#> 1                      Miles per US gallon
#> 2                        Weight (1000 lbs)
#> 3 Motor Trend road tests of 1973-74 models
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
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpQXKWQP/ducklake/ducklake2b6b63925f89.duckdb]
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
#> 1           1 2026-09-05 01:24:14              1
#> 2           2 2026-09-05 01:24:14              2
#> 3          10 2026-09-05 01:24:16             10
#> 4          11 2026-09-05 01:24:16             11
#>                                                              changes
#> 1                 tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_altered, tables_inserted_into, tables_deleted_from, 1, 1, 1
#> 3                                                  tables_altered, 1
#> 4                                                  tables_altered, 1
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
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpQXKWQP/ducklake/ducklake2b6b63925f89.duckdb]
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

### Correct rows in place

``` r

# filter() becomes the WHERE clause of a single UPDATE
with_transaction(
  get_ducklake_table("cars") |>
    filter(cyl == 8) |>
    mutate(hp = hp * 1.02) |>
    ducklake_exec(),
  author = "Data Engineer",
  commit_message = "Apply the dyno correction to V8 engines"
)
#> Transaction started.
#> Transaction committed.
```

### Replace entire table

``` r

# A bulk rewrite that touches most rows: the whole table is rewritten, and
# its comments, partition keys, sort order, and options carry over
with_transaction(
  get_ducklake_table("cars") |>
    mutate(across(c(mpg, kpl), ~ round(.x, 1))) |>
    replace_table("cars"),
  author = "Data Engineer",
  commit_message = "Round fuel efficiency metrics"
)
#> Transaction started.
#> Transaction committed.
```

Note: use the schema evolution functions
([`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)
and friends) for structural changes,
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
and the row-level operations
([`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
[`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
[`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md),
[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md))
for targeted changes, and
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
for bulk rewrites. All are fully versioned: every committed change
creates a snapshot you can time-travel back to. See
[`vignette("modifying-tables")`](https://tgerke.github.io/ducklake-r/articles/modifying-tables.md)
for guidance on choosing between them.

## Metadata and versioning recipes

### View all snapshots

``` r

list_table_snapshots()
#>    snapshot_id       snapshot_time schema_version
#> 1            0 2026-09-05 01:24:13              0
#> 2            1 2026-09-05 01:24:14              1
#> 3            2 2026-09-05 01:24:14              2
#> 4            3 2026-09-05 01:24:14              3
#> 5            4 2026-09-05 01:24:15              4
#> 6            5 2026-09-05 01:24:15              5
#> 7            6 2026-09-05 01:24:15              6
#> 8            7 2026-09-05 01:24:15              7
#> 9            8 2026-09-05 01:24:15              8
#> 10           9 2026-09-05 01:24:15              9
#> 11          10 2026-09-05 01:24:16             10
#> 12          11 2026-09-05 01:24:16             11
#> 13          12 2026-09-05 01:24:16             12
#> 14          13 2026-09-05 01:24:16             12
#> 15          14 2026-09-05 01:24:17             13
#>                                                                                     changes
#> 1                                                                     schemas_created, main
#> 2                                        tables_created, tables_inserted_into, main.cars, 1
#> 3                        tables_altered, tables_inserted_into, tables_deleted_from, 1, 1, 1
#> 4                                 tables_created, tables_inserted_into, main.iris_sample, 2
#> 5                              tables_created, tables_inserted_into, main.efficient_cars, 3
#> 6                                  tables_created, tables_inserted_into, main.small_cars, 4
#> 7                                                                  schemas_created, staging
#> 8                                 tables_created, tables_inserted_into, staging.cars_raw, 6
#> 9                                                      views_created, main.v_efficient_cars
#> 10                                                                         views_dropped, 7
#> 11                                                                        tables_altered, 1
#> 12                                                                        tables_altered, 1
#> 13                        tables_created, tables_altered, inlined_insert, main.visits, 8, 8
#> 14                                          tables_inserted_into, tables_deleted_from, 1, 1
#> 15 tables_created, tables_dropped, tables_altered, tables_inserted_into, main.cars, 1, 9, 9
#>           author                          commit_message commit_extra_info
#> 1           <NA>                                    <NA>              <NA>
#> 2  Data Engineer                   Initial car data load              <NA>
#> 3  Data Engineer           Add km/L metric to cars table              <NA>
#> 4  Data Engineer               Load iris sample from CSV              <NA>
#> 5   Data Analyst                  Load filtered car data              <NA>
#> 6   Data Analyst                    Four-cylinder subset              <NA>
#> 7           <NA>                                    <NA>              <NA>
#> 8           <NA>                                    <NA>              <NA>
#> 9           <NA>                                    <NA>              <NA>
#> 10          <NA>                                    <NA>              <NA>
#> 11          <NA>                                    <NA>              <NA>
#> 12          <NA>                                    <NA>              <NA>
#> 13          <NA>                                    <NA>              <NA>
#> 14 Data Engineer Apply the dyno correction to V8 engines              <NA>
#> 15 Data Engineer           Round fuel efficiency metrics              <NA>
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
#> 1           1 2026-09-05 01:24:14              1
#> 2           2 2026-09-05 01:24:14              2
#> 3          10 2026-09-05 01:24:16             10
#> 4          11 2026-09-05 01:24:16             11
#> 5          13 2026-09-05 01:24:16             12
#> 6          14 2026-09-05 01:24:17             13
#> 7          15 2026-09-05 01:24:17             14
#>                                                                                      changes
#> 1                                         tables_created, tables_inserted_into, main.cars, 1
#> 2                         tables_altered, tables_inserted_into, tables_deleted_from, 1, 1, 1
#> 3                                                                          tables_altered, 1
#> 4                                                                          tables_altered, 1
#> 5                                            tables_inserted_into, tables_deleted_from, 1, 1
#> 6   tables_created, tables_dropped, tables_altered, tables_inserted_into, main.cars, 1, 9, 9
#> 7 tables_created, tables_dropped, tables_altered, tables_inserted_into, main.cars, 9, 10, 10
#>          author                          commit_message commit_extra_info
#> 1 Data Engineer                   Initial car data load              <NA>
#> 2 Data Engineer           Add km/L metric to cars table              <NA>
#> 3          <NA>                                    <NA>              <NA>
#> 4          <NA>                                    <NA>              <NA>
#> 5 Data Engineer Apply the dyno correction to V8 engines              <NA>
#> 6 Data Engineer           Round fuel efficiency metrics              <NA>
#> 7 Data Engineer             Restored cars to snapshot 1              <NA>
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
#> -- DuckLake SQL preview
#> UPDATE cars SET mpg = ROUND_EVEN(mpg, CAST(ROUND(0.0, 0) AS INTEGER));
```

### Filter early for performance

``` r

# Good: Filter before other operations
get_ducklake_table("cars") |>
  filter(cyl == 6) |>
  mutate(kpl = mpg * 0.425144) |>
  head(3)
#> # A query:  ?? x 12
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpQXKWQP/ducklake/ducklake2b6b63925f89.duckdb]
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
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpQXKWQP/ducklake/ducklake2b6b63925f89.duckdb]
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

## Maintenance recipes

### Set a retention policy and checkpoint

``` r

# Keep 90 days of time travel; delete released files a week after release
set_ducklake_option("expire_older_than", "90 days")
set_ducklake_option("delete_older_than", "7 days")

# Flush, compact, and apply the policy
checkpoint_ducklake()
```

Without those two options a checkpoint still flushes inlined data and
compacts files, but expires nothing and deletes nothing. See
[`vignette("storage-and-backups")`](https://tgerke.github.io/ducklake-r/articles/storage-and-backups.md)
for the individual maintenance functions and for backups.

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
