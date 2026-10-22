# Remove a table's override of a DuckLake option

Drops the table-scoped value set with
[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md)
or
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
so the schema or lake-wide setting, or DuckLake's default, applies to
the table again. Runs `ALTER TABLE ... RESET`, the one reset DuckLake
offers: a lake-wide or schema option is changed by setting it again.
Resetting an option the table does not override does nothing. Like the
setter, this works on a table created in the open transaction.

## Usage

``` r
reset_ducklake_option(
  option,
  table_name,
  schema_name = NULL,
  ducklake_name = NULL
)
```

## Arguments

- option:

  Name of the option, as for
  [`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md).

- table_name:

  The table, optionally qualified as `"schema.table"`.

- schema_name:

  Optional schema name that qualifies `table_name`.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

Invisibly returns `NULL`.

## See also

[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md),
[`get_ducklake_options()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_options.md)

Other options:
[`get_ducklake_options()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_options.md),
[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md)

## Examples

``` r
lake_dir <- tempfile("resetopt_lake_")
dir.create(lake_dir)
attach_ducklake("resetopt_lake", lake_path = lake_dir)
create_table(mtcars, "cars", options = list(parquet_compression = "zstd"))

# Back to the lake-wide codec
reset_ducklake_option("parquet_compression", table_name = "cars")
#> Option "parquet_compression" reset for table "cars".

detach_ducklake("resetopt_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
