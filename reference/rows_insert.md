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
  conflict = "ignore",
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

  How to handle conflicts (default "ignore")

- ...:

  Additional arguments passed to dplyr::rows_insert()

## Value

The updated table

## Details

### When to use `rows_*()` vs [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)

Use the `rows_*()` functions for **targeted, incremental changes**:
appending a batch of new records, correcting a handful of values, or
removing specific rows. Each call is a single SQL statement against the
existing table – no data leaves the database, and with data inlining
enabled (DuckLake's default) small changes land in the catalog without
creating tiny Parquet files.

Use
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
for **structural or bulk changes**: adding or removing columns, or
transformations that touch most rows. It collects the transformed data
into R and rewrites the table, which is simpler for schema changes but
heavier for small edits.

## See also

Other row operations:
[`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md),
[`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md)

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
