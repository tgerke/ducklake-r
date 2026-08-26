# Set the comment on a DuckLake table

Stores a description of the table in the DuckLake catalog with
`COMMENT ON TABLE`. Comments live in the lake itself, so every client –
R, Python, or plain SQL – sees the same documentation, and AI tools
reading the catalog get the context too.

## Usage

``` r
set_table_comment(table_name, comment)
```

## Arguments

- table_name:

  The table to describe.

- comment:

  The comment text, or `NULL`/`NA` to clear an existing comment.

## Value

Invisibly returns `NULL`.

## See also

[`set_column_comments()`](https://tgerke.github.io/ducklake-r/reference/set_column_comments.md),
[`get_table_comments()`](https://tgerke.github.io/ducklake-r/reference/get_table_comments.md)

Other table documentation:
[`get_table_comments()`](https://tgerke.github.io/ducklake-r/reference/get_table_comments.md),
[`set_column_comments()`](https://tgerke.github.io/ducklake-r/reference/set_column_comments.md)

## Examples

``` r
lake_dir <- tempfile("comment_lake_")
dir.create(lake_dir)
attach_ducklake("comment_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

set_table_comment("cars", "Motor Trend road tests, one row per model")
#> Commented table "cars".
set_table_comment("cars", NULL) # clear
#> Cleared the comment on "cars".

detach_ducklake("comment_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
