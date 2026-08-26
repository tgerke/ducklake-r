# Detach from a ducklake

Detaches the DuckLake database but keeps the DuckDB connection alive by
default. Use `shutdown = TRUE` to also close the connection and release
file locks.

## Usage

``` r
detach_ducklake(ducklake_name = NULL, shutdown = FALSE)
```

## Arguments

- ducklake_name:

  Optional name of the ducklake to detach.

- shutdown:

  If `TRUE`, shut down the DuckDB connection after detaching. Only
  applies to the connection ducklake created itself; a connection
  registered with
  [`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)
  is never closed for you.

## Value

Invisibly, `NULL`. Called for its side effect of detaching the catalog
from the package's DuckDB connection.

## See also

Other connection management:
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md),
[`ducklake_extension_available()`](https://tgerke.github.io/ducklake-r/reference/ducklake_extension_available.md),
[`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md),
[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
lake_dir <- tempfile("detach_lake_")
dir.create(lake_dir)
attach_ducklake("detach_lake", lake_path = lake_dir)
# ... do work ...
detach_ducklake("detach_lake")

# Full shutdown when completely done
attach_ducklake("detach_lake", lake_path = lake_dir)
detach_ducklake("detach_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
