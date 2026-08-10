# List the tables and views in a DuckLake catalog

Returns every table and view in the lake as a tidy data frame – the
quick answer to "what is in here?".

## Usage

``` r
list_ducklake_tables(ducklake_name = NULL)
```

## Arguments

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

A data frame with one row per object: `schema_name`, `table_name`, and
`type` (`"table"` or `"view"`).

## See also

[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md)
for per-table file statistics,
[`get_table_comments()`](https://tgerke.github.io/ducklake-r/reference/get_table_comments.md)
for stored documentation,
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md)
to read any listed object.

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md),
[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
if (FALSE) { # \dontrun{
list_ducklake_tables()
} # }
```
