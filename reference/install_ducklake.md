# Install the ducklake extension to duckdb

Installs the ducklake DuckDB extension and optionally the extensions for
alternative catalog backends (postgres, sqlite, mysql). Needs to run
once per DuckDB version, provided DuckDB's extension directory survives
the session (see Details).

## Usage

``` r
install_ducklake(backend = NULL)
```

## Arguments

- backend:

  Optional character vector of backends to install. The ducklake
  extension is always installed. Pass `"postgres"`, `"sqlite"`, and/or
  `"mysql"` to install the corresponding backend extensions.

## Value

Invisibly, `NULL`. Called for its side effect of installing the DuckDB
extensions into the local extension cache.

## Details

Where the extension lands depends on the duckdb R package. From duckdb
1.5.2 on, extensions live under a "home" directory that defaults to a
per-session temporary directory unless something durable is configured:
the `DUCKDB_R_HOME` environment variable (set it in `~/.Renviron` so
every session sees it), the `duckdb.home` option, or an existing
`~/.duckdb` directory. With the temporary default, the extension is gone
when R exits and the next session downloads it again.
`install_ducklake()` reports the directory it installed into and says so
when that directory is temporary.

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
[`ducklake_extension_available()`](https://tgerke.github.io/ducklake-r/reference/ducklake_extension_available.md),
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
