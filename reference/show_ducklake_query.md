# Show the SQL that would be executed by ducklake operations

Prints the statement
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
would run for a pipeline, without running it. Like
[`dplyr::show_query()`](https://dplyr.tidyverse.org/reference/explain.html),
the SQL is written to the console and the input is returned invisibly,
so it can sit in the middle of a pipe.

## Usage

``` r
show_ducklake_query(.data, table_name = NULL)
```

## Arguments

- .data:

  A dplyr query object (tbl_lazy)

- table_name:

  The target table name for the operation. If not provided, will be
  extracted from the table attribute (set by get_ducklake_table())

## Value

The first argument, invisibly (following show_query convention)

## See also

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md),
[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)

## Examples

``` r
lake_dir <- tempfile("sql_lake_")
dir.create(lake_dir)
attach_ducklake("sql_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

# Show SQL for an update operation (table name inferred)
get_ducklake_table("cars") |>
  dplyr::mutate(gear = 5) |>
  show_ducklake_query()
#> -- DuckLake SQL preview
#> UPDATE cars SET gear = 5.0;

detach_ducklake("sql_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
