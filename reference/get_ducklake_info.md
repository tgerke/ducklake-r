# Describe an attached DuckLake

One row of facts about a lake: which catalog backend it uses and where
the catalog and data live, the DuckLake format version the catalog is
in, the extension version reading it, whether its Parquet files are
encrypted, and the current snapshot id. Handy at the top of a report or
a pipeline log, and the first thing to look at when a lake behaves
unexpectedly.

## Usage

``` r
get_ducklake_info(ducklake_name = NULL)
```

## Arguments

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

A one-row data frame with `ducklake_name`, `backend`, `catalog` (the
catalog file or connection string), `data_path`, `format_version`,
`extension_version`, `encrypted`, and `current_snapshot`.
`format_version` is `"1.1"` for a lake created with duckdb 2.0 and
`"1.0"` for one created with duckdb 1.5, or with
`ducklake_version = "1.0"`, that has not been migrated; see
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
for what the difference means and how a lake moves from one to the
other.

## See also

[`get_ducklake_options()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_options.md)
for the options set on the lake,
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md)
for what it holds.

Other connection management:
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md),
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`ducklake_extension_available()`](https://tgerke.github.io/ducklake-r/reference/ducklake_extension_available.md),
[`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md),
[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
lake_dir <- tempfile("info_lake_")
dir.create(lake_dir)
attach_ducklake("info_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

get_ducklake_info()
#>   ducklake_name backend
#> 1     info_lake  duckdb
#>                                                     catalog
#> 1 /tmp/RtmpUkUxaF/info_lake_1ad27d693482/info_lake.ducklake
#>                                 data_path format_version extension_version
#> 1 /tmp/RtmpUkUxaF/info_lake_1ad27d693482/            1.1        417a9117f5
#>   encrypted current_snapshot
#> 1     FALSE                1

detach_ducklake("info_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
