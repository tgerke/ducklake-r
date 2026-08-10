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
if (FALSE) { # \dontrun{
set_table_comment("adsl", "Subject-level analysis dataset, one row per subject")
set_table_comment("scratch", NULL)  # clear
} # }
```
