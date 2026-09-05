# Drop a column from a DuckLake table

Removes a column in place with `ALTER TABLE ... DROP COLUMN`. This is a
metadata-only change: the column disappears from the current schema, but
earlier snapshots still contain it and remain queryable through time
travel.

## Usage

``` r
drop_table_column(table_name, column_name)
```

## Arguments

- table_name:

  The table to change.

- column_name:

  Name of the column to drop.

## Value

Invisibly returns `NULL`.

## See also

[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
[`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md),
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md)
to read snapshots that still have the column

Other schema evolution:
[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
[`rename_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/rename_ducklake_table.md),
[`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md),
[`set_column_not_null()`](https://tgerke.github.io/ducklake-r/reference/set_column_not_null.md),
[`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)

## Examples

``` r
lake_dir <- tempfile("dropcol_lake_")
dir.create(lake_dir)
attach_ducklake("dropcol_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

drop_table_column("cars", "carb")
#> Dropped column "carb" from "cars". Earlier snapshots still contain it.

detach_ducklake("dropcol_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
