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

## See also

Other connection management:
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md),
[`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md),
[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
if (FALSE) { # \dontrun{
attach_ducklake("my_ducklake", lake_path = "path/to/lake")
# ... do work ...
detach_ducklake("my_ducklake")

# Full shutdown when completely done
detach_ducklake("my_ducklake", shutdown = TRUE)
} # }
```
