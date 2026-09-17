# Set the comment on a DuckLake table or view

Stores a description of the table or view in the DuckLake catalog with
`COMMENT ON TABLE` or `COMMENT ON VIEW`. Comments live in the lake
itself, so every client – R, Python, or plain SQL – sees the same
documentation, and AI tools reading the catalog get the context too.

## Usage

``` r
set_table_comment(table_name, comment)
```

## Arguments

- table_name:

  The table or view to describe.

- comment:

  The comment text, or `NULL`/`NA` to clear an existing comment.

## Value

Invisibly returns `NULL`.

## Details

[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md)
keeps a view's comment when it replaces the view. A
`CREATE OR REPLACE VIEW` issued any other way drops it, because DuckLake
keys the comment to the catalog entry that the replacement retires.

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

# Views take a comment the same way
get_ducklake_table("cars") |>
  dplyr::filter(cyl == 4) |>
  create_view("v_four_cyl")
#> Created view "v_four_cyl".
set_table_comment("v_four_cyl", "Four-cylinder models")
#> Commented view "v_four_cyl".

detach_ducklake("comment_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
