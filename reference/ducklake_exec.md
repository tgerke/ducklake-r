# Execute DuckLake operations from dplyr queries

Execute DuckLake operations from dplyr queries

## Usage

``` r
ducklake_exec(.data, table_name = NULL, .quiet = TRUE)
```

## Arguments

- .data:

  A dplyr query object (tbl_lazy) with accumulated operations

- table_name:

  The target table name for the operation. If not provided, will be
  extracted from the table attribute (set by get_ducklake_table())

- .quiet:

  Logical, whether to suppress debug output (default TRUE)

## Value

The result from db_execute()

## Details

This function automatically detects the type of operation based on dplyr
verbs:

- Filter-only queries on `table_name` generate DELETE operations
  (removes rows that DON'T match filter)

- Queries with mutate() on `table_name` generate UPDATE operations

- Reads from *other* tables generate INSERT operations, appending their
  result into `table_name` with columns matched by name;
  [`filter()`](https://dplyr.tidyverse.org/reference/filter.html) and
  joins are fine here, since the whole query just feeds the INSERT

A plain read from `table_name` itself is refused, since inserting a
table's own rows back into it would duplicate them. Pipelines that
compile to a subquery over `table_name` (grouped filters,
[`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html) followed
by [`filter()`](https://dplyr.tidyverse.org/reference/filter.html)) are
also refused rather than mistranslated. Use
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)
to preview the generated SQL without running it.

## See also

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Delete rows that don't match filter (table name inferred)
get_ducklake_table("my_table") |>
  filter(status == "inactive") |>
  ducklake_exec()

# Update specific rows (table name inferred)
get_ducklake_table("my_table") |>
  filter(id == 123) |>
  mutate(status = "updated") |>
  ducklake_exec()

# Or provide table name explicitly
tbl(con, "my_table") |>
  select(id, name) |>
  mutate(computed_field = name * 2) |>
  ducklake_exec("my_table")
} # }
```
