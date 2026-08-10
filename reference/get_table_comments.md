# Read table, view, and column comments from a DuckLake catalog

Returns the current comments stored in the lake – via
[`set_table_comment()`](https://tgerke.github.io/ducklake-r/reference/set_table_comment.md),
[`set_column_comments()`](https://tgerke.github.io/ducklake-r/reference/set_column_comments.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)'s
label sync, or any other client – as a tidy data frame.

## Usage

``` r
get_table_comments(table_name = NULL, ducklake_name = NULL)
```

## Arguments

- table_name:

  Optional table (or view) name to filter to.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

A data frame with one row per comment: `object_type` (`"table"`,
`"view"`, or `"column"`), `table_name`, `column_name` (`NA` for tables
and views), and `comment`. Zero rows when nothing is commented.

## See also

[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md)
for the raw `ducklake_tag` and `ducklake_column_tag` catalog tables.

Other table documentation:
[`set_column_comments()`](https://tgerke.github.io/ducklake-r/reference/set_column_comments.md),
[`set_table_comment()`](https://tgerke.github.io/ducklake-r/reference/set_table_comment.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Everything documented in the lake
get_table_comments()

# One table's documentation
get_table_comments("adsl")
} # }
```
