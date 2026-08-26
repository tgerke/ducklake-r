# Rename a DuckLake table

Renames a table in place with `ALTER TABLE ... RENAME TO`, a
metadata-only change.

## Usage

``` r
rename_ducklake_table(from, to)
```

## Arguments

- from:

  Current table name.

- to:

  New table name.

## Value

Invisibly returns `NULL`.

## Details

History survives the rename, but snapshots from before it stay
associated with the old name:
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md)
on the new name reaches back only as far as the rename, while the old
name still serves the earlier snapshots.

## See also

[`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md)

Other schema evolution:
[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
[`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md),
[`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md),
[`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)

## Examples

``` r
lake_dir <- tempfile("renametbl_lake_")
dir.create(lake_dir)
attach_ducklake("renametbl_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

rename_ducklake_table("cars", "cars_daily")
#> Renamed table "cars" to "cars_daily".
#> ℹ Snapshots from before the rename remain queryable under the old name.

detach_ducklake("renametbl_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
