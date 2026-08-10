# Merge a source table into a DuckLake table

Runs a SQL `MERGE INTO` statement: rows of `target` are matched against
rows of `source` on the `by` columns, then updated, deleted, or left
alone, while unmatched source rows can be inserted and target rows
missing from the source can be removed. The whole operation is atomic –
one snapshot, with row lineage preserved in the change feed.

## Usage

``` r
merge_into(
  target,
  source,
  by,
  when_matched = c("update", "delete", "nothing"),
  when_not_matched = c("insert", "nothing"),
  matched_condition = NULL,
  not_matched_condition = NULL,
  delete_missing = FALSE,
  .quiet = TRUE
)
```

## Arguments

- target:

  The table to modify: a table from
  [`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md)
  or a table name.

- source:

  The rows to merge in: a data frame or a lazy table on the same
  connection.

- by:

  Character vector of key column(s) to match on. Rows with `NULL` key
  values never match.

- when_matched:

  What to do with target rows that match a source row: `"update"`
  (default) sets the columns the two tables share, `"delete"` removes
  the row, `"nothing"` leaves it alone.

- when_not_matched:

  What to do with source rows that match no target row: `"insert"`
  (default) adds them, `"nothing"` skips them.

- matched_condition:

  Optional SQL expression further restricting the `when_matched` action,
  written against the aliases `target` and `source`, e.g.
  `"source.amt > target.amt"`.

- not_matched_condition:

  Optional SQL expression further restricting the `when_not_matched`
  action.

- delete_missing:

  Also delete target rows that have no match in the source
  (`WHEN NOT MATCHED BY SOURCE THEN DELETE`). Combined with the update
  action this synchronizes `target` to `source`.

- .quiet:

  Logical, whether to suppress the row-count message (default TRUE).

## Value

The number of affected rows, invisibly.

## Details

This is not a join. Joins
([`left_join()`](https://dplyr.tidyverse.org/reference/mutate-joins.html)
and friends) read from the lake and build a new result without touching
either table; `merge_into()` changes the rows of `target` in place. For
the common update-or-insert case, reach for
[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)
first – `merge_into()` is for the cases it cannot express: conditional
clauses, deletes of matched rows, and synchronizing a table with a
staging source.

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
  statement, use
  [`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md).

- For conditional merge logic or deletes driven by a staging table, use
  `merge_into()`.

- To change a table's shape without touching its data – add, drop, or
  rename columns, widen a type – use the schema evolution family
  ([`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)
  and friends): metadata-only changes that rewrite nothing.

- For bulk transformations that touch most rows, use
  [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md).
  It collects the transformed data into R and rewrites the whole table –
  heavier than the row operations, and it resets the row lineage that
  the in-place operations preserve in the change feed.

A tempting alternative – joining the source to the table and calling
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
on the result – rewrites every row and records the change as a wholesale
replacement. `merge_into()` touches only the affected rows, so
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md)
afterward shows exactly which rows were inserted, updated, or deleted.

### Conditions are SQL

`matched_condition` and `not_matched_condition` are raw SQL expressions,
not dplyr code. They are pasted into the statement as written (only a
`;` is rejected), so build them from trusted input only.

### DuckLake-specific behavior

- Update sets the columns present in both tables (minus `by`); inserted
  rows receive the column default (`NULL` when none is defined) in
  target columns the source lacks.

- `MERGE ... RETURNING` is not implemented by DuckLake.

- DuckLake currently supports one update/delete action per MERGE
  statement. `delete_missing = TRUE` together with a `when_matched`
  action therefore runs as a MERGE plus a `DELETE` of the unmatched
  rows, wrapped in one transaction (a single snapshot). When you already
  opened a transaction, both statements simply join it.

- When several source rows share a `by` key, matched updates apply in an
  unspecified order; keep source keys unique.

## See also

[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)
for the plain update-or-insert case;
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md)
to inspect what a merge did.

Other row operations:
[`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md),
[`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
[`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Update only when the source amount is higher; insert new ids
merge_into(
  get_ducklake_table("sales"), new_sales, by = "sls_id",
  matched_condition = "source.sls_amt > target.sls_amt"
)

# Synchronize to a staging table: upsert + drop rows gone from the source
merge_into(
  "sales", staging_sales, by = "sls_id",
  delete_missing = TRUE
)

# Remove rows flagged in the source
merge_into(
  "sales", withdrawn, by = "sls_id",
  when_matched = "delete", when_not_matched = "nothing"
)
} # }
```
