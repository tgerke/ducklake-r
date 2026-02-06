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
attach_ducklake("my_lake", lake_path = tempdir())
```

### Attach to an existing data lake

``` r
# Attach to an existing lake (creates it if it doesn't exist)
attach_ducklake("existing_lake", lake_path = "/path/to/data_lake")
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
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
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
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
```

### Load data from a CSV file

``` r
# First write a sample CSV (in practice, you'd have an existing file)
csv_path <- file.path(tempdir(), "sample_data.csv")
write.csv(head(iris, 20), csv_path, row.names = FALSE)

# Load the CSV into the data lake
with_transaction(
  create_table(csv_path, "iris_sample"),
  author = "Data Engineer",
  commit_message = "Load iris sample from CSV"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
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
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
```

### List all tables in the lake

``` r
# See what tables exist in your lake
get_ducklake_table("duckdb_tables") |>
  filter(schema_name == "main") |>
  select(table_name) |>
  collect() |>
  print(n = Inf)
#> # A tibble: 25 × 1
#>    table_name                           
#>    <chr>                                
#>  1 ducklake_column                      
#>  2 ducklake_column_mapping              
#>  3 ducklake_column_tag                  
#>  4 ducklake_data_file                   
#>  5 ducklake_delete_file                 
#>  6 ducklake_files_scheduled_for_deletion
#>  7 ducklake_file_column_stats           
#>  8 ducklake_file_partition_value        
#>  9 ducklake_inlined_data_tables         
#> 10 ducklake_metadata                    
#> 11 ducklake_name_mapping                
#> 12 ducklake_partition_column            
#> 13 ducklake_partition_info              
#> 14 ducklake_schema                      
#> 15 ducklake_schema_versions             
#> 16 ducklake_snapshot                    
#> 17 ducklake_snapshot_changes            
#> 18 ducklake_table                       
#> 19 ducklake_table_column_stats          
#> 20 ducklake_table_stats                 
#> 21 ducklake_tag                         
#> 22 ducklake_view                        
#> 23 efficient_cars                       
#> 24 iris_sample                          
#> 25 cars
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
#> # Source:   SQL [?? x 3]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpnOkDzT/duckplyr/duckplyr1fdd3ebc5405.duckdb]
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
#> 2           1 2026-02-06 22:34:27              1
#> 3           2 2026-02-06 22:34:27              2
#>                                                                 changes
#> 2                    tables_created, tables_inserted_into, main.cars, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#>          author                commit_message commit_extra_info
#> 2 Data Engineer         Initial car data load              <NA>
#> 3 Data Engineer Add km/L metric to cars table              <NA>
```

### Read a specific version

``` r
# Query data as it existed at snapshot 1
get_ducklake_table_version("cars", version = 1) |>
  collect()
```

### Read data at a specific timestamp

``` r
# Query data as of a specific time
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
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
```

Note: For most use cases, use
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
to update tables. This creates clean snapshots and maintains full
versioning. Advanced row-level operations (`rows_update`, `rows_insert`,
`rows_delete`, `rows_upsert`) are available when you need granular
control, but they do not create versioned snapshots.

## Metadata and versioning recipes

### List all tables

``` r
get_ducklake_table("duckdb_tables") |>
  filter(schema_name == "main") |>
  select(table_name) |>
  collect()
#> # A tibble: 25 × 1
#>    table_name                           
#>    <chr>                                
#>  1 ducklake_column                      
#>  2 ducklake_column_mapping              
#>  3 ducklake_column_tag                  
#>  4 ducklake_data_file                   
#>  5 ducklake_delete_file                 
#>  6 ducklake_files_scheduled_for_deletion
#>  7 ducklake_file_column_stats           
#>  8 ducklake_file_partition_value        
#>  9 ducklake_inlined_data_tables         
#> 10 ducklake_metadata                    
#> # ℹ 15 more rows
```

### View all snapshots

``` r
list_table_snapshots()
#>   snapshot_id       snapshot_time schema_version
#> 1           0 2026-02-06 22:34:26              0
#> 2           1 2026-02-06 22:34:27              1
#> 3           2 2026-02-06 22:34:27              2
#> 4           3 2026-02-06 22:34:27              3
#> 5           4 2026-02-06 22:34:27              4
#> 6           5 2026-02-06 22:34:28              5
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
# Use time travel to read an old version, then replace the current table
with_transaction(
  get_ducklake_table_version("cars", version = 1) |>
    replace_table("cars"),
  author = "Data Engineer",
  commit_message = "Restore to version 1"
)

list_table_snapshots("cars")
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

``` r
get_ducklake_table("cars") |>
  filter(mpg > 25) |>
  mutate(efficient = TRUE) |>
  show_ducklake_query()
```

### Filter early for performance

``` r
# Good: Filter before other operations
get_ducklake_table("cars") |>
  filter(cyl == 6) |>
  mutate(kpl = mpg * 0.425144) |>
  head(3)
#> # Source:   SQL [?? x 13]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpnOkDzT/duckplyr/duckplyr1fdd3ebc5405.duckdb]
#>     mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb   kpl
#>   <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#> 1  21       6   160   110  3.9   2.62  16.5     0     1     4     4  8.93
#> 2  21       6   160   110  3.9   2.88  17.0     0     1     4     4  8.93
#> 3  21.4     6   258   110  3.08  3.22  19.4     1     0     3     1  9.10
#> # ℹ 1 more variable: hp_per_cyl <dbl>
```

### Use specific columns

``` r
# Good: Select only needed columns
get_ducklake_table("cars") |>
  select(mpg, cyl, hp) |>
  filter(mpg > 25)
#> # Source:   SQL [?? x 3]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpnOkDzT/duckplyr/duckplyr1fdd3ebc5405.duckdb]
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
