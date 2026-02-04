# Upsert rows in a DuckLake table

A wrapper around dplyr::rows_upsert() with in_place = TRUE as the
default, since DuckLake is designed for in-place modifications.

## Usage

``` r
rows_upsert(x, y, by = NULL, copy = TRUE, in_place = TRUE, ...)
```

## Arguments

- x:

  Target table (from get_ducklake_table())

- y:

  Data frame with rows to upsert (update existing, insert new)

- by:

  Column(s) to match on

- copy:

  Whether to copy y to the same source as x (default TRUE)

- in_place:

  Whether to modify the table in place (default TRUE for DuckLake)

- ...:

  Additional arguments passed to dplyr::rows_upsert()

## Value

The updated table

## See also

[`upsert_table()`](https://tgerke.github.io/ducklake-r/reference/upsert_table.md)
for pipeline-based upserts using dplyr transformations

## Examples

``` r
if (FALSE) { # \dontrun{
# Upsert (update if exists, insert if new)
rows_upsert(
  get_ducklake_table("my_table"),
  data.frame(id = c(1, 99), value = c("updated", "new")),
  by = "id"
)
} # }
```
