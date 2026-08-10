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

  How to handle unmatched rows (default "ignore")

- ...:

  Additional arguments passed to dplyr::rows_update()

## Value

The updated table

## Details

### Choosing how to change a table

- To look up or combine data for analysis, use dplyr joins
  ([`left_join()`](https://dplyr.tidyverse.org/reference/mutate-joins.html)
  and friends). Joins read from the lake and build a new result; they
  never modify a lake table.

- To append, correct, or remove specific rows, use
  [`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
  `rows_update()`, or
  [`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md).
  Each call is a single SQL statement against the existing table – no
  data leaves the database, and with data inlining enabled (DuckLake's
  default) small changes land in the catalog without creating tiny
  Parquet files.

- To update rows that exist and insert the ones that don't in one atomic
  statement, use
  [`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md).

- For conditional merge logic or deletes driven by a staging table, use
  [`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md).

- To change a table's shape without touching its data – add, drop, or
  rename columns, widen a type – use the schema evolution family
  ([`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)
  and friends): metadata-only changes that rewrite nothing.

- For bulk transformations that touch most rows, use
  [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md).
  It collects the transformed data into R and rewrites the whole table –
  heavier than the row operations, and it resets the row lineage that
  the in-place operations preserve in the change feed.

## See also

Other row operations:
[`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md),
[`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md),
[`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)

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
