
<!-- README.md is generated from README.Rmd. Please edit that file -->

# ducklake <a href="https://github.com/tgerke/ducklake-r"><img src="man/figures/ducklake-hex.jpg" align="right" height="138" /></a>

ducklake is an R package which complements the existing toolkits in the
[duckdb](https://r.duckdb.org/index.html) and
[duckplyr](https://duckplyr.tidyverse.org/index.html) packages, in order
to support the new
[DuckLake](https://ducklake.select/docs/stable/duckdb/introduction.html)
ecosystem.

## Installation

Install the development version of ducklake with

``` r
pak::pak("tgerke/ducklake-r")
```

## Create a local duckdb lakehouse

``` r
library(ducklake)
library(dplyr)
#> Warning: package 'dplyr' was built under R version 4.5.2

# install the ducklake extension to duckdb 
# requires that you already have DuckDB v1.3.0 or higher
install_ducklake()

# create the ducklake
attach_ducklake("my_ducklake")
# show that we have ducklake files
list.files()
#> [1] "duckplyr"                 "my_ducklake.ducklake"    
#> [3] "my_ducklake.ducklake.wal"

# create a table using the Netherlands train traffic dataset 
create_table("nl_train_stations", "https://blobs.duckdb.org/nl_stations.csv")
# show that we now have a .files directory
list.files()
#> [1] "duckplyr"                   "my_ducklake.ducklake"      
#> [3] "my_ducklake.ducklake.files" "my_ducklake.ducklake.wal"
# main/ is where the parquet files go
list.files("my_ducklake.ducklake.files/main/nl_train_stations")
#> [1] "ducklake-019c2a6d-5307-7601-9e1d-4f3fb2a65ef1.parquet"

# create a table from an R data.frame
create_table("mtcars_table", mtcars)
list.files("my_ducklake.ducklake.files/main/mtcars_table")
#> [1] "ducklake-019c2a6d-5327-7ce6-99e5-5fa4bce0d112.parquet"
```

## Two approaches for table modifications

ducklake provides two complementary approaches for modifying tables,
both following tidyverse conventions:

### 1. data.frame approach (`rows_*` functions)

Best when you have **data in R** (data.frames/tibbles) that you want to
apply to a table:

``` r
# Prepare your data in R
updates <- data.frame(id = c(1, 2), value = c("new1", "new2"))

# Apply to table
rows_update(get_ducklake_table("my_table"), updates, by = "id")  # Update existing rows
rows_insert(get_ducklake_table("my_table"), new_data, by = "id")  # Insert new rows
rows_upsert(get_ducklake_table("my_table"), updates, by = "id")  # Update existing or insert new
rows_delete(get_ducklake_table("my_table"), to_delete, by = "id")  # Delete rows by key
```

**Pros:** Explicit, familiar dplyr syntax, `in_place = TRUE` by default
for DuckLake  
**Use when:** You have data.frames/tibbles ready to apply

### 2. Pipeline approach (`*_table` functions)

Best when you’re **transforming data with dplyr** and want to apply
results to a table:

``` r
# Build transformation pipeline, then execute
get_ducklake_table("my_table") |>
  filter(status == "active") |>
  mutate(processed = TRUE) |>
  ducklake_exec()  # for updates

source_table |>
  select(id, value) |>
  mutate(value = toupper(value)) |>
  upsert_table("target_table", by = "id")  # for merge/upsert
```

**Pros:** Chainable, works in pipelines, table name inference  
**Use when:** Transforming data with `filter()`, `mutate()`,
`summarize()`, etc.

``` r
# update the first row with ducklake::rows_update()
# copy = TRUE and in_place = TRUE are the defaults for DuckLake operations
rows_update(
  get_ducklake_table("nl_train_stations"),
  data.frame(
    uic = 8400319,
    name_short = "NEW"
  ),
  by = "uic",
  copy = TRUE,
  unmatched = "ignore"
)
#> # Source:   SQL [?? x 11]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/Rtmps9ilM8/duckplyr/duckplyr1115535c19b58.duckdb]
#>       id code       uic name_short name_medium     name_long slug  country type 
#>    <dbl> <chr>    <dbl> <chr>      <chr>           <chr>     <chr> <chr>   <chr>
#>  1   269 HTO    8400320 Dn Bosch O 's-Hertogenb. … 's-Herto… s-he… NL      stop…
#>  2   227 HDE    8400388 't Harde   't Harde        't Harde  t-ha… NL      stop…
#>  3     8 AHBF   8015345 Aachen     Aachen Hbf      Aachen H… aach… D       knoo…
#>  4   818 AW     8015199 Aachen W   Aachen West     Aachen W… aach… D       stop…
#>  5    51 ATN    8400045 Aalten     Aalten          Aalten    aalt… NL      stop…
#>  6     5 AC     8400047 Abcoude    Abcoude         Abcoude   abco… NL      stop…
#>  7   550 EAHS   8021123 Ahaus      Ahaus           Ahaus     ahaus D       stop…
#>  8    12 AIME   8774176 Aime-la-Pl Aime-la-Plagne  Aime-la-… aime… F       inte…
#>  9   819 ACDG   8727149 Airport dG Airport deGaul… Airport … airp… F       knoo…
#> 10   551 AIXTGV 8731901 Aix-en-Pro Aix-en-Provence Aix-en-P… aix-… F       inte…
#> # ℹ more rows
#> # ℹ 2 more variables: geo_lat <dbl>, geo_lng <dbl>

# update with mutate and ducklake::ducklake_exec
# table name is automatically inferred from the pipeline
get_ducklake_table("nl_train_stations") |>
  mutate(
    name_long = dplyr::case_when(
      code == "ASB" ~ "Johan Cruijff ArenA",
      .default = name_long
    )
  ) |>
  ducklake_exec()
#> [1] 578

# if we want, we can always view the sql that will be submitted in advance
get_ducklake_table("nl_train_stations") |>
  mutate(
    name_long = dplyr::case_when(
      code == "ASB" ~ "Johan Cruijff ArenA",
      .default = name_long
    )
  ) |>
  show_ducklake_query()
#> 
#> === DuckLake SQL Preview ===
#> 
#> -- Main operation
#> UPDATE nl_train_stations SET name_long = CASE WHEN (code = 'ASB') THEN 'Johan Cruijff ArenA' ELSE name_long END ;

# filter using ducklake::ducklake_exec
# with .quiet=FALSE we can see sql on execution, including the original dplyr
get_ducklake_table("nl_train_stations") |>
  filter(uic == 8400319 | code == "ASB") |>
  ducklake_exec(.quiet = FALSE)
#> 
#> === Original dplyr SQL ===
#> <SQL>
#> SELECT nl_train_stations.*
#> FROM nl_train_stations
#> WHERE (uic = 8400319.0 OR code = 'ASB')
#> # Source:   SQL [?? x 11]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/Rtmps9ilM8/duckplyr/duckplyr1115535c19b58.duckdb]
#>      id code      uic name_short name_medium      name_long  slug  country type 
#>   <dbl> <chr>   <dbl> <chr>      <chr>            <chr>      <chr> <chr>   <chr>
#> 1   266 HT    8400319 Den Bosch  's-Hertogenbosch 's-Hertog… s-he… NL      knoo…
#> 2    41 ASB   8400074 Bijlmer A  Bijlmer ArenA    Johan Cru… amst… NL      knoo…
#> # ℹ 2 more variables: geo_lat <dbl>, geo_lng <dbl>
#> 
#> === Translated DuckLake SQL ===
#> DELETE FROM nl_train_stations WHERE NOT ((uic = 8400319.0 OR code = 'ASB')) 
#> 
#> Rows affected: 576
#> [1] 576

# show our current table
get_ducklake_table("nl_train_stations")
#> # Source:   table<nl_train_stations> [?? x 11]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/Rtmps9ilM8/duckplyr/duckplyr1115535c19b58.duckdb]
#>      id code      uic name_short name_medium      name_long  slug  country type 
#>   <dbl> <chr>   <dbl> <chr>      <chr>            <chr>      <chr> <chr>   <chr>
#> 1   266 HT    8400319 Den Bosch  's-Hertogenbosch 's-Hertog… s-he… NL      knoo…
#> 2    41 ASB   8400074 Bijlmer A  Bijlmer ArenA    Johan Cru… amst… NL      knoo…
#> # ℹ 2 more variables: geo_lat <dbl>, geo_lng <dbl>
```

## Upsert (merge) data

``` r
# Upsert: update existing rows or insert new ones based on a key
# First, create some data to upsert
upsert_data <- data.frame(
  uic = c(8400319, 9999999),  # 8400319 exists, 9999999 is new
  name_short = c("UPDATED", "NEW"),
  name_long = c("Updated Station", "New Station"),
  code = c("UPD", "NEW"),
  stringsAsFactors = FALSE
)

# Use rows_upsert for data.frames (copy = TRUE and in_place = TRUE by default)
rows_upsert(
  get_ducklake_table("nl_train_stations"),
  upsert_data,
  by = "uic",
  copy = TRUE
)
#> # Source:   SQL [?? x 11]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/Rtmps9ilM8/duckplyr/duckplyr1115535c19b58.duckdb]
#>      id code      uic name_short name_medium      name_long  slug  country type 
#>   <dbl> <chr>   <dbl> <chr>      <chr>            <chr>      <chr> <chr>   <chr>
#> 1    41 ASB   8400074 Bijlmer A  Bijlmer ArenA    Johan Cru… amst… NL      knoo…
#> 2   266 UPD   8400319 UPDATED    's-Hertogenbosch Updated S… s-he… NL      knoo…
#> 3    NA NEW   9999999 NEW        <NA>             New Stati… <NA>  <NA>    <NA> 
#> # ℹ 2 more variables: geo_lat <dbl>, geo_lng <dbl>

get_ducklake_table("nl_train_stations") |>
  select(uic, name_short, name_long, code)
#> # Source:   SQL [?? x 4]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/Rtmps9ilM8/duckplyr/duckplyr1115535c19b58.duckdb]
#>       uic name_short name_long           code 
#>     <dbl> <chr>      <chr>               <chr>
#> 1 8400319 Den Bosch  's-Hertogenbosch    HT   
#> 2 8400074 Bijlmer A  Johan Cruijff ArenA ASB
```

## View metadata and snapshots

``` r
# List all tables in the lake
get_ducklake_table("duckdb_tables") |> 
  select(database_name, schema_name, table_name) |> 
  print(n = Inf)
#> # Source:   SQL [?? x 3]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/Rtmps9ilM8/duckplyr/duckplyr1115535c19b58.duckdb]
#>    database_name                   schema_name table_name                       
#>    <chr>                           <chr>       <chr>                            
#>  1 __ducklake_metadata_my_ducklake main        ducklake_column                  
#>  2 __ducklake_metadata_my_ducklake main        ducklake_column_mapping          
#>  3 __ducklake_metadata_my_ducklake main        ducklake_column_tag              
#>  4 __ducklake_metadata_my_ducklake main        ducklake_data_file               
#>  5 __ducklake_metadata_my_ducklake main        ducklake_delete_file             
#>  6 __ducklake_metadata_my_ducklake main        ducklake_files_scheduled_for_del…
#>  7 __ducklake_metadata_my_ducklake main        ducklake_file_column_stats       
#>  8 __ducklake_metadata_my_ducklake main        ducklake_file_partition_value    
#>  9 __ducklake_metadata_my_ducklake main        ducklake_inlined_data_tables     
#> 10 __ducklake_metadata_my_ducklake main        ducklake_metadata                
#> 11 __ducklake_metadata_my_ducklake main        ducklake_name_mapping            
#> 12 __ducklake_metadata_my_ducklake main        ducklake_partition_column        
#> 13 __ducklake_metadata_my_ducklake main        ducklake_partition_info          
#> 14 __ducklake_metadata_my_ducklake main        ducklake_schema                  
#> 15 __ducklake_metadata_my_ducklake main        ducklake_schema_versions         
#> 16 __ducklake_metadata_my_ducklake main        ducklake_snapshot                
#> 17 __ducklake_metadata_my_ducklake main        ducklake_snapshot_changes        
#> 18 __ducklake_metadata_my_ducklake main        ducklake_table                   
#> 19 __ducklake_metadata_my_ducklake main        ducklake_table_column_stats      
#> 20 __ducklake_metadata_my_ducklake main        ducklake_table_stats             
#> 21 __ducklake_metadata_my_ducklake main        ducklake_tag                     
#> 22 __ducklake_metadata_my_ducklake main        ducklake_view                    
#> 23 my_ducklake                     main        mtcars_table                     
#> 24 my_ducklake                     main        nl_train_stations                
#> 25 temp                            main        dbplyr_A3Da8r21wL                
#> 26 temp                            main        dbplyr_AbFZZXOzFd

# View snapshot history
get_metadata_table("ducklake_snapshot_changes", ducklake_name = "my_ducklake")
#> # Source:   SQL [?? x 5]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/Rtmps9ilM8/duckplyr/duckplyr1115535c19b58.duckdb]
#>   snapshot_id changes_made               author commit_message commit_extra_info
#>         <dbl> <chr>                      <chr>  <chr>          <chr>            
#> 1           0 "created_schema:\"main\""  <NA>   <NA>           <NA>             
#> 2           1 "created_table:\"main\".\… <NA>   <NA>           <NA>             
#> 3           2 "created_table:\"main\".\… <NA>   <NA>           <NA>             
#> 4           3 "inserted_into_table:1,de… <NA>   <NA>           <NA>             
#> 5           4 "deleted_from_table:1"     <NA>   <NA>           <NA>
get_metadata_table("ducklake_snapshot", ducklake_name = "my_ducklake")
#> # Source:   SQL [?? x 5]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/Rtmps9ilM8/duckplyr/duckplyr1115535c19b58.duckdb]
#>   snapshot_id snapshot_time       schema_version next_catalog_id next_file_id
#>         <dbl> <dttm>                       <dbl>           <dbl>        <dbl>
#> 1           0 2026-02-04 20:52:13              0               1            0
#> 2           1 2026-02-04 20:52:13              1               2            1
#> 3           2 2026-02-04 20:52:13              2               3            2
#> 4           3 2026-02-04 20:52:13              2               3            3
#> 5           4 2026-02-04 20:52:14              2               3            4
```

## Cleanup

``` r
# When done, detach from the ducklake
detach_ducklake("my_ducklake")
#> Warning in value[[3L]](cond): Could not detach ducklake: Invalid Error: Binder Error: Cannot detach database "my_ducklake" because it is the default database. Select a different database using `USE` to allow detaching this database
#> ℹ Context: rapi_execute
#> ℹ Error type: INVALID
```
