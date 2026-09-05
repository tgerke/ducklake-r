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

  Logical, whether to suppress the SQL trace (default TRUE). With
  `.quiet = FALSE` the original dplyr SQL, the translated statement, and
  the number of rows affected are emitted as messages.

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
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md),
[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
lake_dir <- tempfile("exec_lake_")
dir.create(lake_dir)
attach_ducklake("exec_lake", lake_path = lake_dir)
create_table(data.frame(id = 1:3, status = "pending"), "jobs")

# Update specific rows (table name inferred)
get_ducklake_table("jobs") |>
  dplyr::filter(id == 1) |>
  dplyr::mutate(status = "updated") |>
  ducklake_exec()
#> [1] 1

# Delete rows matching a filter
get_ducklake_table("jobs") |>
  dplyr::filter(status == "pending") |>
  ducklake_exec()
#> [1] 1

# Or provide the table name explicitly
get_ducklake_table("jobs") |>
  dplyr::mutate(status = "done") |>
  ducklake_exec("jobs")
#> [1] 2

detach_ducklake("exec_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
