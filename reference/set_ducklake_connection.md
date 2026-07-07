# Use your own DuckDB connection with ducklake

By default, ducklake creates and manages its own DuckDB connection. Call
this function to make ducklake use a connection you have created instead
– for example, a connection you share with duckplyr or other DBI-based
tools, or one configured with custom DuckDB settings.

## Usage

``` r
set_ducklake_connection(conn)
```

## Arguments

- conn:

  A live DuckDB connection created with
  [DBI::dbConnect()](https://dbi.r-dbi.org/reference/dbConnect.html) and
  [`duckdb::duckdb()`](https://r.duckdb.org/reference/duckdb.html).

## Value

The connection, invisibly.

## Details

ducklake never closes a connection you supply:
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md)
with `shutdown = TRUE` and the end-of-session cleanup only shut down
connections that ducklake created itself. Closing your connection
remains your responsibility.

If ducklake was already managing its own connection, that connection is
shut down before yours is registered.

## See also

[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md)

Other connection management:
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md),
[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)

## Examples

``` r
if (FALSE) { # \dontrun{
conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = "my_analysis.duckdb")
set_ducklake_connection(conn)
attach_ducklake("my_lake", lake_path = "~/lakes/my_lake")
} # }
```
