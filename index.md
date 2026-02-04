# ducklake

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

## Quick example

``` r
library(ducklake)
library(dplyr)

# Install the ducklake extension (requires DuckDB v1.3.0 or higher)
install_ducklake()

# Create a lakehouse
attach_ducklake("my_ducklake")

# Create a table
create_table("nl_train_stations", "https://blobs.duckdb.org/nl_stations.csv")

# View the data
get_ducklake_table("nl_train_stations") |>
  select(code, name_short, name_long) |>
  head(5)
#> # Source:   SQL [?? x 3]
#> # Database: DuckDB 1.4.4 [tgerke@Darwin 23.6.0:R 4.5.2//private/var/folders/b7/664jmq55319dcb7y4jdb39zr0000gq/T/Rtmpm6OHxc/duckplyr/duckplyr143e97f37364.duckdb]
#>   code  name_short name_long            
#>   <chr> <chr>      <chr>                
#> 1 HT    Den Bosch  's-Hertogenbosch     
#> 2 HTO   Dn Bosch O 's-Hertogenbosch Oost
#> 3 HDE   't Harde   't Harde             
#> 4 AHBF  Aachen     Aachen Hbf           
#> 5 AW    Aachen W   Aachen West

# Update with dplyr syntax
get_ducklake_table("nl_train_stations") |>
  mutate(name_short = toupper(name_short)) |>
  ducklake_exec()
#> [1] 578

# Clean up
detach_ducklake("my_ducklake")
```

## Learn more

Check out the [pkgdown site](https://tgerke.github.io/ducklake-r/) for
detailed vignettes:

- [Getting
  Started](https://tgerke.github.io/ducklake-r/articles/ducklake.html) -
  Create your first lakehouse
- [Modifying
  Tables](https://tgerke.github.io/ducklake-r/articles/modifying-tables.html) -
  Two approaches for table modifications
- [Upsert
  Operations](https://tgerke.github.io/ducklake-r/articles/upsert-operations.html) -
  Merge and update data
- [Transactions](https://tgerke.github.io/ducklake-r/articles/transactions.html) -
  ACID transaction support
- [Time
  Travel](https://tgerke.github.io/ducklake-r/articles/time-travel.html) -
  Query historical data

## Key features

- **Tidyverse-style interface** for DuckLake operations
- **Two complementary approaches**: `rows_*` functions for data.frames
  and pipeline functions for dplyr workflows
- **ACID transactions** with metadata tracking
- **Time travel queries** to access historical snapshots
- **Seamless integration** with duckdb and duckplyr
