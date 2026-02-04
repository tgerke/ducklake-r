
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
#> [1] "ducklake-019c2a4e-4192-75fb-b5d7-67c99335ac94.parquet"
```

## Update tables

``` r
# update the first row with dplyr::rows_update
rows_update(
  get_ducklake_table("nl_train_stations"),
  data.frame(
    uic = 8400319,
    name_short = "NEW"
  ),
  by = "uic",
  copy = TRUE,
  in_place = TRUE,
  unmatched = "ignore"
)

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
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/RtmpYhvUdl/duckplyr/duckplyrfe70671cc150.duckdb]
#>      id code      uic name_short name_medium      name_long  slug  country type 
#>   <dbl> <chr>   <dbl> <chr>      <chr>            <chr>      <chr> <chr>   <chr>
#> 1   266 HT    8400319 NEW        's-Hertogenbosch 's-Hertog… s-he… NL      knoo…
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
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/RtmpYhvUdl/duckplyr/duckplyrfe70671cc150.duckdb]
#>      id code      uic name_short name_medium      name_long  slug  country type 
#>   <dbl> <chr>   <dbl> <chr>      <chr>            <chr>      <chr> <chr>   <chr>
#> 1   266 HT    8400319 NEW        's-Hertogenbosch 's-Hertog… s-he… NL      knoo…
#> 2    41 ASB   8400074 Bijlmer A  Bijlmer ArenA    Johan Cru… amst… NL      knoo…
#> # ℹ 2 more variables: geo_lat <dbl>, geo_lng <dbl>
```

## View metadata and snapshots

``` r
# List all tables in the lake
get_ducklake_table("duckdb_tables") |> 
  select(database_name, schema_name, table_name) |> 
  print(n = Inf)
#> # Source:   SQL [?? x 3]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/RtmpYhvUdl/duckplyr/duckplyrfe70671cc150.duckdb]
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
#> 23 my_ducklake                     main        nl_train_stations                
#> 24 temp                            main        dbplyr_6NrOmqtRRe

# View snapshot history
get_metadata_table("ducklake_snapshot_changes", ducklake_name = "my_ducklake")
#> # Source:   SQL [?? x 5]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/RtmpYhvUdl/duckplyr/duckplyrfe70671cc150.duckdb]
#>   snapshot_id changes_made               author commit_message commit_extra_info
#>         <dbl> <chr>                      <chr>  <chr>          <chr>            
#> 1           0 "created_schema:\"main\""  <NA>   <NA>           <NA>             
#> 2           1 "created_table:\"main\".\… <NA>   <NA>           <NA>             
#> 3           2 "inserted_into_table:1,de… <NA>   <NA>           <NA>             
#> 4           3 "inserted_into_table:1,de… <NA>   <NA>           <NA>             
#> 5           4 "deleted_from_table:1"     <NA>   <NA>           <NA>
get_metadata_table("ducklake_snapshot", ducklake_name = "my_ducklake")
#> # Source:   SQL [?? x 5]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.1//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/RtmpYhvUdl/duckplyr/duckplyrfe70671cc150.duckdb]
#>   snapshot_id snapshot_time       schema_version next_catalog_id next_file_id
#>         <dbl> <dttm>                       <dbl>           <dbl>        <dbl>
#> 1           0 2026-02-04 20:18:17              0               1            0
#> 2           1 2026-02-04 20:18:17              1               2            1
#> 3           2 2026-02-04 20:18:17              1               2            3
#> 4           3 2026-02-04 20:18:17              1               2            4
#> 5           4 2026-02-04 20:18:17              1               2            5
```
