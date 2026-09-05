# Drop a schema from a DuckLake

Removes a schema with `DROP SCHEMA`. By default the schema must be
empty; `cascade = TRUE` drops its tables and views with it. Like every
change, the drop is a snapshot, so the tables stay reachable through
time travel.

## Usage

``` r
drop_schema(schema_name, cascade = FALSE, ducklake_name = NULL)
```

## Arguments

- schema_name:

  Name of the schema.

- cascade:

  Also drop the tables and views in the schema (default `FALSE`).

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

Invisibly, `NULL`.

## See also

[`create_schema()`](https://tgerke.github.io/ducklake-r/reference/create_schema.md)

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`create_schema()`](https://tgerke.github.io/ducklake-r/reference/create_schema.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md),
[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
lake_dir <- tempfile("dropschema_lake_")
dir.create(lake_dir)
attach_ducklake("dropschema_lake", lake_path = lake_dir)

create_schema("scratch")
#> Created schema "scratch".
create_table(mtcars, "scratch.cars")
drop_schema("scratch", cascade = TRUE)
#> Dropped schema "scratch".

detach_ducklake("dropschema_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
