# Update rows in a DuckLake table

A wrapper around dplyr::rows_update() with in_place = TRUE as the
default, since DuckLake is designed for in-place modifications.

## Usage

``` r
rows_update(
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

  Data frame with updates

- by:

  Column(s) to match on

- copy:

  Whether to copy y to the same source as x (default TRUE)

- in_place:

  Whether to modify the table in place (default TRUE for DuckLake)

- unmatched:

  How to handle unmatched rows (default "error")

- ...:

  Additional arguments passed to dplyr::rows_update()

## Value

The updated table

## Examples

``` r
if (FALSE) { # \dontrun{
# Update rows - in_place = TRUE by default
rows_update(
  get_ducklake_table("my_table"),
  data.frame(id = 1, value = "new"),
  by = "id"
)
} # }
```
