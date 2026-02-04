# Patch rows in a DuckLake table

A wrapper around dplyr::rows_patch() with in_place = TRUE as the
default, since DuckLake is designed for in-place modifications.

## Usage

``` r
rows_patch(
  x,
  y,
  by = NULL,
  copy = TRUE,
  in_place = TRUE,
  unmatched = "error",
  ...
)
```

## Arguments

- x:

  Target table (from get_ducklake_table())

- y:

  Data frame with patches (only updates non-NA values)

- by:

  Column(s) to match on

- copy:

  Whether to copy y to the same source as x (default TRUE)

- in_place:

  Whether to modify the table in place (default TRUE for DuckLake)

- unmatched:

  How to handle unmatched rows (default "error")

- ...:

  Additional arguments passed to dplyr::rows_patch()

## Value

The updated table

## Examples

``` r
if (FALSE) { # \dontrun{
# Patch (only update non-NA columns)
rows_patch(
  get_ducklake_table("my_table"),
  data.frame(id = 1, col1 = "update", col2 = NA),
  by = "id"
)
} # }
```
