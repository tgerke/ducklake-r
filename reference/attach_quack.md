# Connect to a remote Quack server

Attaches a remote Quack server as a catalog in the current session.
Tables in the server's default database are then reachable as
`quack_name.table_name` and can be queried with
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md)
or [`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html).

## Usage

``` r
attach_quack(
  quack_name,
  uri,
  token = NULL,
  disable_ssl = FALSE,
  ssl_fingerprint = NULL,
  client_id = NULL,
  heartbeat_timeout = NULL
)
```

## Arguments

- quack_name:

  Name for the attached remote catalog, used as the database alias in
  DuckDB.

- uri:

  Address of the Quack server, for example `"quack:localhost"` or
  `"quack:data.example.org:9494"`. A bare host such as `"localhost"` is
  prefixed with `quack:` automatically. The default port is 9494.

- token:

  Authentication token expected by the server. If `NULL`, the token is
  taken from a Quack secret (see `CREATE SECRET`) if one exists.

- disable_ssl:

  Connect over plain HTTP instead of HTTPS (default `FALSE`, which
  leaves the choice to DuckDB: plain HTTP for a local server, HTTPS
  otherwise). Only appropriate on a trusted network.

- ssl_fingerprint:

  Optional SHA-256 fingerprint (64 hex digits) of the server's
  certificate, for a server with a self-signed certificate; the value
  [`quack_generate_keys()`](https://tgerke.github.io/ducklake-r/reference/quack_generate_keys.md)
  returns on the server side.

- client_id:

  Optional identifier the server records for this client (at least 4
  characters), shown by its `quack_active_connections()`.

- heartbeat_timeout:

  Optional heartbeat lease, in seconds, requested from the server.

## Value

Invisibly, `NULL`. Called for its side effect of attaching the remote
catalog to the package's DuckDB connection.

## Details

A DuckLake served over Quack lives in its own catalog on the server
rather than in the default database, so its tables are not exposed under
`quack_name`. Query a served DuckLake with
[`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md),
naming the lake's catalog, for example
`quack_query(uri, "SELECT * FROM trial.adsl")`. To work with a lake
whose catalog a Quack server holds open, attach it with
`attach_ducklake(backend = "quack")` instead.

DuckDB 2.0's `CONNECT` statement, which points a whole session at a
server, is not used here and should not be run on the package's
connection: on the duckdb R client it leaves the connection unable to
prepare any further statement
(<https://github.com/duckdb/duckdb/issues/27269>).

## See also

[`detach_quack()`](https://tgerke.github.io/ducklake-r/reference/detach_quack.md),
[`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md),
[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md)

Other quack:
[`detach_quack()`](https://tgerke.github.io/ducklake-r/reference/detach_quack.md),
[`install_quack()`](https://tgerke.github.io/ducklake-r/reference/install_quack.md),
[`quack_generate_keys()`](https://tgerke.github.io/ducklake-r/reference/quack_generate_keys.md),
[`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md),
[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md),
[`quack_stop()`](https://tgerke.github.io/ducklake-r/reference/quack_stop.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# A remote DuckDB database, queried through the attached catalog
attach_quack("warehouse", "quack:data.example.org", token = "super_secret")

get_ducklake_table("warehouse.sales") |>
  dplyr::filter(region == "EMEA") |>
  dplyr::collect()

detach_quack("warehouse")
} # }
```
