# Create a schema in a DuckLake

Schemas group the tables of a lake, and are the natural home for
medallion layers (`bronze`, `silver`, `gold`) or per-study areas. Every
function that takes a table name accepts `"schema.table"`, and
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md)
shows each table's schema. DuckLake writes a schema's data files under a
directory of their own, which is what path-based access control on
object storage keys on.

## Usage

``` r
create_schema(schema_name, if_not_exists = TRUE, ducklake_name = NULL)
```

## Arguments

- schema_name:

  Name of the schema.

- if_not_exists:

  When the schema already exists, say so and do nothing (default `TRUE`)
  rather than error.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

Invisibly, `NULL`.

## See also

[`drop_schema()`](https://tgerke.github.io/ducklake-r/reference/drop_schema.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md)

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
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
lake_dir <- tempfile("schema_lake_")
dir.create(lake_dir)
attach_ducklake("schema_lake", lake_path = lake_dir)

# One schema per medallion layer
create_schema("bronze")
#> Created schema "bronze".
create_schema("silver")
#> Created schema "silver".

create_table(mtcars, "bronze.cars")
get_ducklake_table("bronze.cars") |>
  dplyr::filter(cyl == 4) |>
  create_table("silver.cars")

list_ducklake_tables()
#>   schema_name table_name  type
#> 1      bronze       cars table
#> 2      silver       cars table

detach_ducklake("schema_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
