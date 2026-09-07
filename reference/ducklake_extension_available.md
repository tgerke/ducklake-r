# Is the ducklake DuckDB extension usable?

Reports whether the `ducklake` DuckDB extension is already installed and
can be loaded. The check opens a throwaway in-memory DuckDB connection,
asks DuckDB's extension catalog whether the extension is installed, and
loads it only if so. Reading the catalog cannot download anything, and
loading an installed extension does not reach the network, so the probe
never downloads and never writes to the extension directory. The
connection is opened on the directory duckdb would resolve for a new
connection, so an existing `~/.duckdb` is found, and the offer duckdb
makes to create one is not triggered. The package's own connection is
left untouched.

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
scripts. The package's own examples are gated on it. There is no need to
call it before
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
which installs the extension the first time it is needed;
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)
downloads it ahead of time.

A `FALSE` on a machine where the extension was installed earlier usually
means it went into a temporary directory. duckdb keeps extensions under
a "home" directory: the `duckdb.home` option or `DUCKDB_R_HOME` when
set, otherwise `~/.duckdb` when it exists (in an interactive session
duckdb offers to create it the first time it connects), otherwise a
per-session temporary directory. See
[`?duckdb::duckdb_storage`](https://r.duckdb.org/reference/duckdb_storage.html).

The result is cached for the rest of the session, since spinning up
DuckDB to re-answer the same question is wasteful when dozens of
examples ask it.
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)
and a first-use install by
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
clear the cache, so a `FALSE` answer becomes `TRUE` as soon as the
extension is there.

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
  message("attach_ducklake() will download it on first use")
}
#> ducklake extension is ready to use
```
