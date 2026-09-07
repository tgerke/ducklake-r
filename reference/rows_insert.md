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

### Choosing how to change a table

- To look up or combine data for analysis, use dplyr joins
  ([`left_join()`](https://dplyr.tidyverse.org/reference/mutate-joins.html)
  and friends). Joins read from the lake and build a new result; they
  never modify a lake table.

- To append, correct, or remove specific rows, use `rows_insert()`,
  [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
  or
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
  It rewrites the whole table inside DuckDB – heavier than the row
  operations, and it resets the row lineage that the in-place operations
  preserve in the change feed.

## See also

Other row operations:
[`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md),
[`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md),
[`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)

## Examples

``` r
lake_dir <- tempfile("rowsins_lake_")
dir.create(lake_dir)
attach_ducklake("rowsins_lake", lake_path = lake_dir)
create_table(data.frame(id = 1:3, value = c("a", "b", "c")), "items")

rows_insert(
  get_ducklake_table("items"),
  data.frame(id = 99, value = "new row"),
  by = "id"
)

detach_ducklake("rowsins_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
