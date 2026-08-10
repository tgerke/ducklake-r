# Upsert rows into a DuckLake table

Updates rows of `x` that match a row of `y` (by the `by` columns) and
inserts the rows of `y` that have no match, as one atomic `MERGE INTO`
statement – a single snapshot, with row lineage preserved in the change
feed. A wrapper around dplyr::rows_upsert() with in_place = TRUE as the
default, since DuckLake is designed for in-place modifications.

## Usage

``` r
rows_upsert(x, y, by = NULL, copy = TRUE, in_place = TRUE, ...)
```

## Arguments

- x:

  Target table (from get_ducklake_table())

- y:

  Data frame with rows to update or insert

- by:

  Column(s) to match on. Defaults to the first column of `y`, with a
  message.

- copy:

  Whether to copy y to the same source as x (default TRUE)

- in_place:

  Whether to modify the table in place (default TRUE for DuckLake)

- ...:

  Additional arguments passed to dplyr::rows_upsert()

## Value

The updated table, invisibly

## Details

### Choosing how to change a table

- To look up or combine data for analysis, use dplyr joins
  ([`left_join()`](https://dplyr.tidyverse.org/reference/mutate-joins.html)
  and friends). Joins read from the lake and build a new result; they
  never modify a lake table.

- To append, correct, or remove specific rows, use
  [`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
  [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
  or
  [`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md).
  Each call is a single SQL statement against the existing table – no
  data leaves the database, and with data inlining enabled (DuckLake's
  default) small changes land in the catalog without creating tiny
  Parquet files.

- To update rows that exist and insert the ones that don't in one atomic
  statement, use `rows_upsert()`.

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

### DuckLake-specific behavior

DuckLake tables have no primary keys or unique constraints, so the usual
database upsert (`INSERT ... ON CONFLICT`) does not apply. This method
instead generates `MERGE INTO`, matching on the `by` columns:

- When `y` covers a subset of `x`'s columns, matched rows are updated in
  those columns only; inserted rows receive the column's default value
  in the remaining columns (`NULL` when the table defines none). Note
  the difference from data-frame upserts, which fill with `NA`.

- When `y` has only the `by` columns, there is nothing to update and the
  call inserts the unmatched rows.

- Rows where a `by` column is `NULL` never match and are always
  inserted.

- When several rows of `y` share the same `by` key, each matched update
  applies in an unspecified order; the last write wins. Keep `y` keys
  unique.

## See also

[`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md)
for conditional merge clauses and source-driven deletes.

Other row operations:
[`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md),
[`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md),
[`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
[`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Update id 2, insert id 4 - one statement, one snapshot
rows_upsert(
  get_ducklake_table("my_table"),
  data.frame(id = c(2, 4), value = c("updated", "new")),
  by = "id"
)
} # }
```
