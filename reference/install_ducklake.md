# Install the ducklake extension to duckdb

Installs the ducklake DuckDB extension and optionally the extensions for
alternative catalog backends (postgres, sqlite, mysql). Only needs to be
run once per DuckDB version.

## Usage

``` r
install_ducklake(backend = NULL)
```

## Arguments

- backend:

  Optional character vector of backends to install. The ducklake
  extension is always installed. Pass `"postgres"`, `"sqlite"`, and/or
  `"mysql"` to install the corresponding backend extensions.

## Note

On Windows the `postgres` and `mysql` extensions are not available
(MinGW toolchain). See
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
for details.

## See also

Other connection management:
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md),
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md),
[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
if (FALSE) { # \dontrun{
install_ducklake()
install_ducklake(backend = "postgres")
install_ducklake(backend = c("postgres", "sqlite", "mysql"))
} # }
```
