# Create a DuckLake table

Create a DuckLake table

## Usage

``` r
create_table(data_source, table_name, labels = TRUE)
```

## Arguments

- data_source:

  Raw data source. Can be:

  - A URL (http:// or https://)

  - A file path (e.g., "data.csv", "data.parquet")

  - An R data.frame or tibble

  - A lazy table (tbl_duckdb_connection or tbl_lazy)

- table_name:

  Name of the new table

- labels:

  When `TRUE` (the default) and the data has haven/labelled variable
  labels (`label` attributes on columns), store them in the lake as
  column comments – in the same transaction as the table creation, so
  both land as one snapshot. Collecting the table later restores the
  labels (see
  [`get_table_comments()`](https://tgerke.github.io/ducklake-r/reference/get_table_comments.md)),
  and every other client of the lake can read them too. Set to `FALSE`
  to skip.

## See also

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
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
if (FALSE) { # \dontrun{
# From URL
create_table("https://example.com/data.csv", "my_table")

# From local file
create_table("data.csv", "my_table")

# From data.frame
create_table(mtcars, "my_table")

# From lazy table (pipe-friendly)
get_ducklake_table("source_table") %>% 
  filter(x > 5) %>%
  create_table("filtered_table")
} # }
```
