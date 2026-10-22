# Create a DuckLake table

Create a DuckLake table

## Usage

``` r
create_table(
  data_source,
  table_name,
  labels = TRUE,
  partition_by = NULL,
  sort_by = NULL,
  options = NULL
)
```

## Arguments

- data_source:

  Raw data source. Can be:

  - A URL (http:// or https://)

  - A file path (e.g., "data.csv", "data.parquet")

  - An R data.frame or tibble

  - A lazy table (tbl_duckdb_connection or tbl_lazy). A lazy table on
    the package's connection, such as a dplyr pipeline built on
    [`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
    is written with `CREATE TABLE ... AS` inside DuckDB, so its rows
    never pass through R. A lazy table on another connection is
    collected first.

- table_name:

  Name of the new table

- labels:

  When `TRUE` (the default), store variable labels in the lake as column
  comments, in the same transaction as the table creation, so both land
  as one snapshot. For a data frame the labels are its haven/labelled
  `label` attributes; for a lazy table, each output column keeps the
  comment of the same-named column in the tables the query reads, so
  labels follow the data through a pipeline (a renamed or derived column
  starts without one). Collecting the table later restores the labels
  (see
  [`get_table_comments()`](https://tgerke.github.io/ducklake-r/reference/get_table_comments.md)),
  and every other client of the lake can read them too. Set to `FALSE`
  to skip.

- partition_by:

  Optional character vector of partition expressions, as for
  [`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md):
  column names, `year(col)` and the other calendar transforms, or
  `bucket(n, col)`. The keys go on before the rows, so the first load is
  already partitioned, where
  [`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md)
  on an existing table affects only later writes.

- sort_by:

  Optional character vector of sort expressions, as for
  [`set_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/set_table_sorting.md):
  a column name, optionally followed by `ASC` or `DESC` and
  `NULLS FIRST` or `NULLS LAST`. The first load is written in this
  order.

- options:

  Optional named list of table-scoped options, as for
  [`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md),
  for example
  `list(parquet_compression = "zstd", data_inlining_row_limit = 0)`.
  They are set before the rows are written and so govern the first load
  too: an inlining limit of `0` here sends even a handful of rows to a
  Parquet file instead of the catalog.

## Value

Invisibly, `NULL`. Called for its side effect of creating the table in
the lake.

## Details

With any of `partition_by`, `sort_by`, or `options`, the table is
created empty from the source's columns, the keys and options go on, and
the rows are inserted, in one transaction (yours when one is open), so
the result is one snapshot with the layout in force from the first row.
Without them, a single `CREATE TABLE ... AS` does the work.

A column that is `NA` in every row of a query (`mutate(flag = NA)`) has
no type for DuckDB to infer, and the table stores it with the `"NULL"`
type, which holds nothing but `NULL` until
[`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)
promotes it. Give the placeholder a type in the query (`NA_character_`,
`NA_real_`) when the first values are known.

## See also

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`create_schema()`](https://tgerke.github.io/ducklake-r/reference/create_schema.md),
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md),
[`drop_schema()`](https://tgerke.github.io/ducklake-r/reference/drop_schema.md),
[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
lake_dir <- tempfile("create_lake_")
dir.create(lake_dir)
attach_ducklake("create_lake", lake_path = lake_dir)

# From data.frame
create_table(mtcars, "cars")

# From a local file
csv_path <- tempfile(fileext = ".csv")
utils::write.csv(mtcars, csv_path, row.names = FALSE)
create_table(csv_path, "cars_from_csv")

# From a lazy table: the query runs inside DuckDB and writes straight
# into the lake, without collecting into R
get_ducklake_table("cars") |>
  dplyr::filter(cyl > 4) |>
  create_table("big_cars")

# Partitioned, sorted, and compressed from the first load
create_table(
  mtcars, "cars_by_cyl",
  partition_by = "cyl", sort_by = "mpg DESC",
  options = list(parquet_compression = "zstd")
)
get_table_partitions("cars_by_cyl")
#>   schema_name  table_name partition_key_index column_name transform
#> 1        main cars_by_cyl                   0         cyl  identity

# From a URL -- needs network access and the httpfs extension
if (FALSE) { # \dontrun{
create_table("https://example.com/data.csv", "remote_table")
} # }

unlink(csv_path)

detach_ducklake("create_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
