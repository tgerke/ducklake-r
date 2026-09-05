# Is the ducklake DuckDB extension usable?

Reports whether the `ducklake` DuckDB extension is already installed and
can be loaded. The check opens a throwaway in-memory DuckDB connection
with automatic extension installation turned off, so it never downloads
anything and never writes to the extension cache in your home directory.
It also leaves the package's own connection untouched.

## Usage

``` r
ducklake_extension_available()
```

## Value

A single logical: `TRUE` when the extension loads, `FALSE` otherwise
(including when a connection cannot be opened).

## Details

Use it to guard code that should degrade gracefully when the extension
is absent: examples, vignettes, tests, and conditional branches in
scripts. The package's own examples are gated on it. To install the
extension, call
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md).

A `FALSE` on a machine where
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)
has already run usually means the extension went into a temporary
directory: from duckdb 1.5.2 on, extensions are kept under a "home"
directory that defaults to a per-session temporary directory unless
`DUCKDB_R_HOME` (or the `duckdb.home` option, or an existing
`~/.duckdb`) points somewhere durable. See
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md).

The result is cached for the rest of the session, since spinning up
DuckDB to re-answer the same question is wasteful when dozens of
examples ask it.
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)
clears the cache, so a `FALSE` answer becomes `TRUE` as soon as you
install.

## See also

Other connection management:
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md),
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md),
[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
[`get_ducklake_info()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_info.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
if (ducklake_extension_available()) {
  message("ducklake extension is ready to use")
} else {
  message("run install_ducklake() first")
}
#> ducklake extension is ready to use
```
