# Set or drop a column's default value

Changes what later inserts put in a column they leave out, with
`ALTER TABLE ... ALTER COLUMN ... SET DEFAULT` (or `DROP DEFAULT`), a
metadata-only change. Existing rows keep their values; the default in
[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)
is the one that fills existing rows.

## Usage

``` r
set_column_default(table_name, column_name, default)
```

## Arguments

- table_name:

  The table to change.

- column_name:

  Name of the column.

- default:

  The new default: an R scalar (character, numeric, logical, Date, or
  POSIXct in UTC), written as a typed literal; a
  [`DBI::SQL()`](https://dbi.r-dbi.org/reference/SQL.html) string for an
  expression DuckDB evaluates at insert time, such as
  `DBI::SQL("now()")`; or `NULL` to drop the default.

## Value

Invisibly returns `NULL`.

## Details

An expression default needs a committed column: DuckLake refuses it on a
column added in the open transaction, so add the column, commit, and
then set the default. A constant default has no such limit.

## See also

[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
[`set_column_not_null()`](https://tgerke.github.io/ducklake-r/reference/set_column_not_null.md)

Other schema evolution:
[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
[`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md),
[`rename_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/rename_ducklake_table.md),
[`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md),
[`set_column_not_null()`](https://tgerke.github.io/ducklake-r/reference/set_column_not_null.md),
[`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)

## Examples

``` r
create_table(data.frame(id = 1:2, note = c("a", "b")), "events")

add_table_column("events", "recorded_at", "TIMESTAMP")
#> Added column "recorded_at" (TIMESTAMP) to "events".
#> ℹ Metadata-only change; no data files were rewritten.
set_column_default("events", "recorded_at", DBI::SQL("now()"))
#> Column "recorded_at" in "events" defaults to `now()` for new rows.
set_column_default("events", "note", "none")
#> Column "note" in "events" defaults to `'none'` for new rows.
set_column_default("events", "note", NULL)
#> Column "note" in "events" has no default now.
```
