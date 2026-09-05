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

  Optional table (or view) name to filter to, optionally qualified as
  `"schema.table"`; a bare name matches that table in every schema.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

A data frame with one row per comment: `object_type` (`"table"`,
`"view"`, or `"column"`), `schema_name`, `table_name`, `column_name`
(`NA` for tables and views), and `comment`. Zero rows when nothing is
commented.

## See also

[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md)
for the raw `ducklake_tag` and `ducklake_column_tag` catalog tables.

Other table documentation:
[`set_column_comments()`](https://tgerke.github.io/ducklake-r/reference/set_column_comments.md),
[`set_table_comment()`](https://tgerke.github.io/ducklake-r/reference/set_table_comment.md)

## Examples

``` r
lake_dir <- tempfile("readcomment_lake_")
dir.create(lake_dir)
attach_ducklake("readcomment_lake", lake_path = lake_dir)
create_table(mtcars, "cars")
set_table_comment("cars", "Motor Trend road tests")
#> Commented table "cars".
set_column_comments("cars", mpg = "Miles per US gallon")
#> Commented 1 column on "cars".

# Everything documented in the lake
get_table_comments()
#>   object_type schema_name table_name column_name                comment
#> 1      column        main       cars         mpg    Miles per US gallon
#> 2       table        main       cars        <NA> Motor Trend road tests

# One table's documentation
get_table_comments("cars")
#>   object_type schema_name table_name column_name                comment
#> 1      column        main       cars         mpg    Miles per US gallon
#> 2       table        main       cars        <NA> Motor Trend road tests

detach_ducklake("readcomment_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
