# Get the DuckDB connection used by ducklake

Returns the DuckDB connection that all ducklake functions share. The
first call creates the connection automatically, so you never need to
set one up yourself. If you want ducklake to use a connection you have
created (for example, one shared with other tools), register it first
with
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md).

## Usage

``` r
get_ducklake_connection()
```

## Value

A DuckDB connection object (a `duckdb_connection`).

## Details

The automatically created connection is backed by a temporary database
file (not `:memory:`) with a spill directory configured, so
larger-than-memory operations work out of the box. It is closed
automatically when the R session ends.

## See also

Other connection management:
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
[`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md),
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
if (FALSE) { # \dontrun{
conn <- get_ducklake_connection()
DBI::dbGetQuery(conn, "SELECT version()")
} # }
```
