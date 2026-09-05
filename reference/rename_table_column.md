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
[`set_column_not_null()`](https://tgerke.github.io/ducklake-r/reference/set_column_not_null.md),
[`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)

## Examples

``` r
lake_dir <- tempfile("renamecol_lake_")
dir.create(lake_dir)
attach_ducklake("renamecol_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

rename_table_column("cars", from = "mpg", to = "miles_per_gallon")
#> Renamed column "mpg" to "miles_per_gallon" in "cars".

detach_ducklake("renamecol_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
