# Rename a column in a DuckLake table

Renames a column in place with `ALTER TABLE ... RENAME COLUMN`, a
metadata-only change.

## Usage

``` r
rename_table_column(table_name, from, to)
```

## Arguments

- table_name:

  The table to change.

- from:

  Current column name.

- to:

  New column name.

## Value

Invisibly returns `NULL`.

## See also

[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
[`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md),
[`rename_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/rename_ducklake_table.md)

Other schema evolution:
[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
[`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md),
[`rename_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/rename_ducklake_table.md),
[`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)

## Examples

``` r
if (FALSE) { # \dontrun{
rename_table_column("adsl", from = "AGEGRP", to = "AGEGR1")
} # }
```
