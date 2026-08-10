# Set column comments on a DuckLake table

Stores per-column descriptions in the DuckLake catalog with
`COMMENT ON COLUMN`, one `name = "comment"` pair per column. For data
with haven/labelled variable labels,
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
can store the labels automatically; this function adds or revises them
afterward – for example to label a derived variable.

## Usage

``` r
set_column_comments(table_name, ...)
```

## Arguments

- table_name:

  The table whose columns to describe.

- ...:

  Named comments, e.g. `USUBJID = "Unique subject identifier"`. Use `NA`
  (or `NULL`) as a value to clear that column's comment. To pass a named
  vector built elsewhere, splice it with
  `do.call(set_column_comments, c(list("tbl"), as.list(my_labels)))`.

## Value

Invisibly returns `NULL`.

## Details

All comments from one call are written in a single transaction, so they
land as one snapshot. Inside
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
they join the open transaction instead.

## See also

[`set_table_comment()`](https://tgerke.github.io/ducklake-r/reference/set_table_comment.md),
[`get_table_comments()`](https://tgerke.github.io/ducklake-r/reference/get_table_comments.md)

Other table documentation:
[`get_table_comments()`](https://tgerke.github.io/ducklake-r/reference/get_table_comments.md),
[`set_table_comment()`](https://tgerke.github.io/ducklake-r/reference/set_table_comment.md)

## Examples

``` r
if (FALSE) { # \dontrun{
set_column_comments(
  "adsl",
  USUBJID = "Unique subject identifier",
  AGEGR1 = "Age group 1",
  SCRATCH = NA  # clear this one
)
} # }
```
