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

## Examples

``` r
if (FALSE) { # \dontrun{
install_ducklake()
install_ducklake(backend = "postgres")
install_ducklake(backend = c("postgres", "sqlite", "mysql"))
} # }
```
