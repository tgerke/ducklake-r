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

  Replace an existing view of the same name (default TRUE).

## Value

Invisibly returns `NULL`.

## Details

Read a view back with
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
which works for views and tables alike, and keep piping dplyr verbs onto
it. Like tables, views are versioned: dropping or replacing one is a
snapshot like any other.

## See also

[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
to materialize a pipeline as data instead.

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Encapsulate filtering logic the whole team should share
get_ducklake_table("adsl") |>
  filter(SAFFL == "Y") |>
  select(USUBJID, TRT01A, AGE) |>
  create_view("v_safety_population")

# Reads run the stored query against current data
get_ducklake_table("v_safety_population") |> collect()
} # }
```
