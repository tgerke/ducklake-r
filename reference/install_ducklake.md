# Download the ducklake extension ahead of time

[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
installs the ducklake DuckDB extension the first time it is needed, so
most sessions never call this function. It downloads the extension now,
and optionally the extensions for the other catalog backends (postgres,
sqlite, mysql), for the cases where the download has to happen before a
lake is attached: baking a container image, a CI setup step, or a
machine that is offline at attach time.

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

Where the extension lands is decided by the duckdb R package (see
[`?duckdb::duckdb_storage`](https://r.duckdb.org/reference/duckdb_storage.html)).
Extensions live under a "home" directory: the `duckdb.home` option or
`DUCKDB_R_HOME` when set, otherwise `~/.duckdb` when it exists (in an
interactive session duckdb offers to create it the first time it
connects), otherwise a per-session temporary directory. With the
temporary directory the extension is gone when R exits and the next
session downloads it again. `install_ducklake()` reports the directory
it installed into and says so when that directory is temporary. For
scripts and CI, set `DUCKDB_R_HOME` in `~/.Renviron` or the job's
environment to a directory that survives the session.

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
[`get_ducklake_info()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_info.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
if (FALSE) { # \dontrun{
install_ducklake()
install_ducklake(backend = "postgres")
install_ducklake(backend = c("postgres", "sqlite", "mysql"))
} # }
```
