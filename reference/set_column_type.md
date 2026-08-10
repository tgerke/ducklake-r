# Change the type of a DuckLake table column

Changes a column's type in place with
`ALTER TABLE ... ALTER COLUMN ... SET TYPE`. DuckLake permits widening
promotions only (for example `INTEGER` to `BIGINT`, or `FLOAT` to
`DOUBLE`): every existing value must be representable in the new type,
so no data can be lost and no data files need rewriting.

## Usage

``` r
set_column_type(table_name, column_name, type)
```

## Arguments

- table_name:

  The table to change.

- column_name:

  Name of the column.

- type:

  The new (wider) SQL type.

## Value

Invisibly returns `NULL`.

## Details

To *narrow* a type, which DuckLake refuses, take the explicit route:
[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)
with the smaller type, copy the values over (after checking they fit),
[`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md)
the original, and
[`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md)
the new column into place.

## See also

[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
[`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md),
[`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md)

Other schema evolution:
[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
[`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md),
[`rename_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/rename_ducklake_table.md),
[`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md)

## Examples

``` r
if (FALSE) { # \dontrun{
set_column_type("sales", "order_id", "BIGINT")
} # }
```
