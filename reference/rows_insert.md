# Insert rows into a DuckLake table

A wrapper around dplyr::rows_insert() with in_place = TRUE as the
default, since DuckLake is designed for in-place modifications.

## Usage

``` r
rows_insert(
  x,
  y,
  by = NULL,
  copy = TRUE,
  in_place = TRUE,
  conflict = "error",
  ...
)
```

## Arguments

- x:

  Target table (from get_ducklake_table())

- y:

  Data frame with new rows

- by:

  Column(s) to match on (for conflict detection)

- copy:

  Whether to copy y to the same source as x (default TRUE)

- in_place:

  Whether to modify the table in place (default TRUE for DuckLake)

- conflict:

  How to handle conflicts (default "error")

- ...:

  Additional arguments passed to dplyr::rows_insert()

## Value

The updated table

## Examples

``` r
if (FALSE) { # \dontrun{
rows_insert(
  get_ducklake_table("my_table"),
  data.frame(id = 99, value = "new row"),
  by = "id"
)
} # }
```
