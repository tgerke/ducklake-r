# Add a column to a DuckLake table

Adds a column in place with `ALTER TABLE ... ADD COLUMN`. This is a
metadata-only change: no data files are rewritten, history is preserved,
and earlier snapshots still show the old schema. Compare
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
which rewrites the whole table.

## Usage

``` r
add_table_column(table_name, column_name, type, default = NULL)
```

## Arguments

- table_name:

  The table to change.

- column_name:

  Name of the new column.

- type:

  SQL type for the new column, e.g. `"INTEGER"`, `"DECIMAL(10,2)"`, or
  `"TIMESTAMP WITH TIME ZONE"`.

- default:

  Optional default value (an R scalar: character, numeric, logical,
  Date, or POSIXct, the last rendered in UTC). In DuckLake the default
  applies to existing rows as well as future inserts, so the new column
  appears filled everywhere. Without a default, the column reads `NA`
  for existing rows. DuckLake accepts constant defaults only; an
  expression such as `now()` is not supported.

## Value

Invisibly returns `NULL`.

## See also

[`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md),
[`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md),
[`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md),
[`rename_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/rename_ducklake_table.md)

Other schema evolution:
[`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md),
[`rename_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/rename_ducklake_table.md),
[`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md),
[`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)

## Examples

``` r
lake_dir <- tempfile("addcol_lake_")
dir.create(lake_dir)
attach_ducklake("addcol_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

add_table_column("cars", "grade", "VARCHAR")
#> Added column "grade" (VARCHAR) to "cars".
#> ℹ Metadata-only change; no data files were rewritten.
add_table_column("cars", "discount", "DECIMAL(5,2)", default = 0)
#> Added column "discount" (DECIMAL(5,2)) to "cars".
#> ℹ Metadata-only change; no data files were rewritten.

detach_ducklake("addcol_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
