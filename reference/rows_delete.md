# Delete rows from a DuckLake table

A wrapper around dplyr::rows_delete() with in_place = TRUE as the
default, since DuckLake is designed for in-place modifications.

## Usage

``` r
rows_delete(
  x,
  y,
  by = NULL,
  copy = TRUE,
  in_place = TRUE,
  unmatched = "ignore",
  ...
)
```

## Arguments

- x:

  Target table (from get_ducklake_table())

- y:

  Data frame with rows to delete (matched by 'by' columns)

- by:

  Column(s) to match on

- copy:

  Whether to copy y to the same source as x (default TRUE)

- in_place:

  Whether to modify the table in place (default TRUE for DuckLake)

- unmatched:

  How to handle unmatched rows (default "error")

- ...:

  Additional arguments passed to dplyr::rows_delete()

## Value

The updated table

## Examples

``` r
if (FALSE) { # \dontrun{
rows_delete(
  get_ducklake_table("my_table"),
  data.frame(id = c(1, 2, 3)),
  by = "id"
)
} # }
```
