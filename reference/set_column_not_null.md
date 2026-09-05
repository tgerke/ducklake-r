# Require or allow NULL values in a column

Sets or drops a `NOT NULL` constraint with
`ALTER TABLE ... ALTER COLUMN ... SET NOT NULL` (or `DROP NOT NULL`), a
metadata-only change. `NOT NULL` is the one constraint DuckLake
supports: there are no primary keys, unique constraints, or check
constraints, which is why
[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)
matches on its `by` columns instead of a key. Existing rows must already
satisfy the constraint, and from then on an insert or update that would
leave the column `NULL` is refused.

## Usage

``` r
set_column_not_null(table_name, column_name, not_null = TRUE)
```

## Arguments

- table_name:

  The table to change.

- column_name:

  Name of the column.

- not_null:

  `TRUE` (the default) to require a value in every row, `FALSE` to allow
  `NULL` again.

## Value

Invisibly returns `NULL`.

## See also

[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
[`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)

Other schema evolution:
[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
[`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md),
[`rename_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/rename_ducklake_table.md),
[`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md),
[`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)

## Examples

``` r
lake_dir <- tempfile("notnull_lake_")
dir.create(lake_dir)
attach_ducklake("notnull_lake", lake_path = lake_dir)
create_table(data.frame(id = 1:3, code = c("A", "B", "C")), "sites")

set_column_not_null("sites", "code")
#> Column "code" in "sites" now requires a value.

# A row without a code is now refused
try(rows_insert(get_ducklake_table("sites"), data.frame(id = 4L), by = "id"))
#> Error in dplyr::rows_insert(x = x, y = y, by = by, ..., conflict = conflict,  : 
#>   Can't modify database table "sites".
#> ℹ Using SQL: INSERT INTO sites (id) SELECT * FROM ( SELECT TRY_CAST(id AS
#>   INTEGER) AS id FROM ( SELECT NULL AS id WHERE (0 = 1)
#> 
#> UNION ALL
#> 
#> VALUES (4) ) AS values_table ) AS "...y" WHERE NOT EXISTS ( SELECT 1 FROM sites
#>   WHERE (sites.id = "...y".id) )
#> Caused by error in `duckdb_result()`:
#> ! Invalid Error: Constraint Error: NOT NULL constraint failed: sites.code
#> ℹ Context: rapi_execute
#> ℹ Error type: INVALID

set_column_not_null("sites", "code", not_null = FALSE)
#> Column "code" in "sites" allows NULL again.

detach_ducklake("notnull_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
