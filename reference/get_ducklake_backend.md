# Get the catalog backend type of an attached lake

Get the catalog backend type of an attached lake

## Usage

``` r
get_ducklake_backend(ducklake_name = NULL)
```

## Arguments

- ducklake_name:

  Name of the lake to look up. When `NULL` (the default), the lake the
  session is currently `USE`ing is looked up.

## Value

One of `"duckdb"`, `"postgres"`, `"sqlite"`, or `"mysql"`. Defaults to
`"duckdb"` when the lake is unknown.

## See also

Other connection management:
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md),
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`ducklake_extension_available()`](https://tgerke.github.io/ducklake-r/reference/ducklake_extension_available.md),
[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
[`get_ducklake_info()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_info.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
lake_dir <- tempfile("backend_lake_")
dir.create(lake_dir)
attach_ducklake("backend_lake", lake_path = lake_dir)

get_ducklake_backend()
#> [1] "duckdb"

# With several lakes attached, look one up by name
get_ducklake_backend("backend_lake")
#> [1] "duckdb"

detach_ducklake("backend_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
