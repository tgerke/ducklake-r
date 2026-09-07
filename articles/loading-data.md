# Loading Data

``` r

library(ducklake)
library(dplyr)

attach_ducklake("loading_lake", lake_path = vignette_temp_dir)
```

[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
is the one entry point for data that is not in the lake yet. It accepts
a data frame, a path to a file, a URL, or a lazy table, and in every
case the result is a new table and a new snapshot. The recipes below
start with the everyday cases and end with the migration paths:
registering Parquet files that already exist, and copying whole
databases in from DuckDB or Iceberg.

Each load here is wrapped in
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
so that the snapshot carries an author and a message. [Getting
Started](https://tgerke.github.io/ducklake-r/articles/ducklake.md)
explains why.

## From a data frame

``` r

with_transaction(
  create_table(mtcars, "cars"),
  author = "Data Engineer",
  commit_message = "Add the Motor Trend car data"
)
#> Committed snapshot 1 (Data Engineer): Add the Motor Trend car data
```

Factor columns become character columns on the way in, because DuckLake
has no `ENUM` type, and
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
says so when it happens:

``` r

with_transaction(
  create_table(iris, "flowers"),
  author = "Data Engineer",
  commit_message = "Add the iris measurements"
)
#> Converted factor column Species to character (DuckLake does not support ENUM
#> columns).
#> Committed snapshot 2 (Data Engineer): Add the iris measurements
```

Variable labels (the `label` attributes that haven and labelled use) are
stored as column comments and come back on
[`collect()`](https://dplyr.tidyverse.org/reference/compute.html).
[Views, Comments, and
Labels](https://tgerke.github.io/ducklake-r/articles/views-comments-labels.md)
shows the round trip.

## From a file

A file path goes straight to DuckDB’s readers, so CSV, Parquet, and JSON
files all load with the same call:

``` r

csv_path <- file.path(vignette_temp_dir, "sample_data.csv")
write.csv(head(iris, 20), csv_path, row.names = FALSE)

with_transaction(
  create_table(csv_path, "iris_sample"),
  author = "Data Engineer",
  commit_message = "Load the iris sample from CSV"
)
#> Committed snapshot 3 (Data Engineer): Load the iris sample from CSV

get_ducklake_table("iris_sample") |> head(3)
#> # A query:  ?? x 5
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpxgANjZ/ducklake/ducklake2c4e58a565d0.duckdb]
#>   Sepal.Length Sepal.Width Petal.Length Petal.Width Species
#>          <dbl>       <dbl>        <dbl>       <dbl> <chr>  
#> 1          5.1         3.5          1.4         0.2 setosa 
#> 2          4.9         3            1.4         0.2 setosa 
#> 3          4.7         3.2          1.3         0.2 setosa
```

## From a URL

An `http://` or `https://` address works the same way. ducklake loads
DuckDB’s `httpfs` extension for it:

``` r

with_transaction(
  create_table("https://example.com/data.csv", "remote_data"),
  author = "Data Engineer",
  commit_message = "Load the remote dataset"
)
```

## From a dplyr pipeline

A pipeline on an R data frame is collected and written like any other
data frame:

``` r

with_transaction(
  mtcars |>
    filter(mpg > 20) |>
    create_table("efficient_cars"),
  author = "Data Analyst",
  commit_message = "Add the cars above 20 mpg"
)
#> Committed snapshot 4 (Data Analyst): Add the cars above 20 mpg
```

## Derive a table inside the database

When the pipeline starts from a lake table,
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
runs it as `CREATE TABLE ... AS` inside DuckDB. The rows never pass
through R, and column labels follow the columns into the new table:

``` r

with_transaction(
  get_ducklake_table("cars") |>
    filter(cyl == 4) |>
    select(mpg, cyl, hp, wt) |>
    create_table("small_cars"),
  author = "Data Analyst",
  commit_message = "Four-cylinder subset"
)
#> Committed snapshot 5 (Data Analyst): Four-cylinder subset
```

This is the shape of a medallion pipeline: each layer is a query on the
layer before it, written straight into the lake.

## Put tables in schemas

Schemas group tables inside the lake. They suit medallion layers
(`bronze`, `silver`, `gold`) or one area per study, and every function
that takes a table name accepts `"schema.table"`:

``` r

with_transaction({
  create_schema("staging")
  create_table(mtcars, "staging.cars_raw")
},
  author = "Data Engineer",
  commit_message = "Stage the raw car data"
)
#> Created schema "staging".
#> Committed snapshot 6 (Data Engineer): Stage the raw car data

get_ducklake_table("staging.cars_raw") |>
  count(cyl)
#> # A query:  ?? x 2
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpxgANjZ/ducklake/ducklake2c4e58a565d0.duckdb]
#>     cyl     n
#>   <dbl> <dbl>
#> 1     4    11
#> 2     6     7
#> 3     8    14

list_ducklake_tables()
#>   schema_name     table_name  type
#> 1        main           cars table
#> 2        main efficient_cars table
#> 3        main        flowers table
#> 4        main    iris_sample table
#> 5        main     small_cars table
#> 6     staging       cars_raw table
```

## Register Parquet files in place

If your data is already in Parquet,
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md)
records the files in the lake where they are: no copy, no rewrite, and
nothing collected into R. A vector of files is registered atomically in
one snapshot, which makes this the fast migration path for a folder of
Parquet extracts. The target table can already exist with a compatible
schema, or `create = TRUE` creates it from the Parquet schema. The lake
takes ownership of the files, and later compaction may rewrite or delete
them.

``` r

add_data_files(
  "readings",
  c("extracts/jan.parquet", "extracts/feb.parquet"),
  create = TRUE
)

# See which files back a table
list_ducklake_files("readings")
```

## Migrate a DuckDB database

A DuckDB database file moves into the lake with one statement: attach it
read-only and copy every schema and table across. The copy lands as a
single snapshot.

``` r

legacy_db <- file.path(vignette_temp_dir, "legacy.duckdb")
legacy <- DBI::dbConnect(duckdb::duckdb(dbdir = legacy_db))
DBI::dbWriteTable(
  legacy, "sites",
  data.frame(site_id = 1:3, country = c("US", "DE", "JP"))
)
DBI::dbDisconnect(legacy, shutdown = TRUE)

conn <- get_ducklake_connection()
DBI::dbExecute(conn, sprintf("ATTACH '%s' AS legacy (READ_ONLY);", legacy_db))
#> [1] 0
DBI::dbExecute(conn, "COPY FROM DATABASE legacy TO loading_lake;")
#> [1] 0
DBI::dbExecute(conn, "DETACH legacy;")
#> [1] 0

get_ducklake_table("sites") |> collect()
#> # A tibble: 3 × 2
#>   site_id country
#>     <int> <chr>  
#> 1       1 US     
#> 2       2 DE     
#> 3       3 JP
```

Types DuckLake does not support (`ENUM`, `UNION`, `VARINT`, fixed-size
arrays) need converting first; the DuckLake documentation has a
migration script for those cases:
<https://ducklake.select/docs/stable/duckdb/migrations/duckdb_to_ducklake>.

## Exchange data with Iceberg

With DuckDB’s iceberg extension attached to an Iceberg REST catalog,
`COPY FROM DATABASE` moves tables in either direction. Copying into
Iceberg needs the schemas to exist there first, and the copy adds tables
rather than replacing them.

``` r

DBI::dbExecute(conn, "INSTALL iceberg; LOAD iceberg;")
DBI::dbExecute(conn, "
  ATTACH '' AS iceberg_catalog (
    TYPE iceberg,
    CLIENT_ID 'admin',
    CLIENT_SECRET 'password',
    ENDPOINT 'http://iceberg.example.org:8181'
  );
")

# Lake to Iceberg
DBI::dbExecute(conn, "COPY FROM DATABASE loading_lake TO iceberg_catalog;")

# Iceberg to lake: the data itself, or only the metadata, so that Iceberg
# tables read as lake tables where they are
DBI::dbExecute(conn, "COPY FROM DATABASE iceberg_catalog TO loading_lake;")
DBI::dbExecute(conn, "CALL iceberg_to_ducklake('iceberg_catalog', 'loading_lake');")
```

``` r

detach_ducklake("loading_lake")
```
