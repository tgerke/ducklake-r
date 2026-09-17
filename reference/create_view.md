# Create a DuckLake view from a dplyr pipeline

Stores a dplyr pipeline in the lake as a SQL view: the query runs fresh
every time the view is read, so it always reflects the current data.
Views live in the DuckLake catalog itself, which makes them a good home
for shared business logic – a Python or SQL client of the same lake sees
exactly the same definition.

## Usage

``` r
create_view(.data, view_name, replace = TRUE)
```

## Arguments

- .data:

  A lazy table (a dplyr pipeline built on
  [`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md)).
  Not a data frame: a view stores a query, not data – use
  [`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
  to store data.

- view_name:

  Name for the view.

- replace:

  Replace an existing view of the same name (default TRUE). The replaced
  view's comment carries over.

## Value

Invisibly returns `NULL`.

## Details

Read a view back with
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
which works for views and tables alike, and keep piping dplyr verbs onto
it. Like tables, views are versioned: dropping or replacing one is a
snapshot like any other.

DuckLake stores a replaced view as a new catalog entry and keys the
comment to the entry, so `CREATE OR REPLACE VIEW` by itself drops the
comment. `create_view()` reads the comment first and sets it again, in
the same snapshot as the replacement, the way
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
carries a table's comments over. Reword it with
[`set_table_comment()`](https://tgerke.github.io/ducklake-r/reference/set_table_comment.md),
or clear it with `NULL`.

A view kept in a schema of its own should read its tables by
schema-qualified name: `get_ducklake_table("main.cars")`, not `"cars"`.
DuckDB resolves an unqualified name from the view's schema and the
session's current database, so the view can fail to bind in a session
where another database is current, and a view that shares its name with
the table it reads (`checks.cars` over `cars`) reads itself and fails
with a recursion error.

## See also

[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
to materialize a pipeline as data instead.

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`create_schema()`](https://tgerke.github.io/ducklake-r/reference/create_schema.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
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
lake_dir <- tempfile("view_lake_")
dir.create(lake_dir)
attach_ducklake("view_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

# Encapsulate filtering logic the whole team should share
get_ducklake_table("cars") |>
  dplyr::filter(cyl == 4) |>
  dplyr::select(mpg, cyl, gear) |>
  create_view("v_efficient_cars")
#> Created view "v_efficient_cars".

# Reads run the stored query against current data
get_ducklake_table("v_efficient_cars") |> dplyr::collect()
#> # A tibble: 11 × 3
#>      mpg   cyl  gear
#>    <dbl> <dbl> <dbl>
#>  1  22.8     4     4
#>  2  24.4     4     4
#>  3  22.8     4     4
#>  4  32.4     4     4
#>  5  30.4     4     4
#>  6  33.9     4     4
#>  7  21.5     4     3
#>  8  27.3     4     4
#>  9  26       4     5
#> 10  30.4     4     5
#> 11  21.4     4     4

detach_ducklake("view_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
