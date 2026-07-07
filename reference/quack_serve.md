# Serve the current session over Quack

Starts a Quack server in the current DuckDB instance. Everything
attached to the session, including a DuckLake attached with
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
becomes reachable by other DuckDB clients over the `quack:` protocol.
The server runs in the background of the DuckDB instance, so the R
session stays usable.

## Usage

``` r
quack_serve(
  uri = "quack:localhost",
  token = NULL,
  allow_other_hostname = FALSE,
  disable_ssl = FALSE
)
```

## Arguments

- uri:

  Address to listen on (default `"quack:localhost"`). The default port
  is 9494.

- token:

  Authentication token that clients must supply. If `NULL`, the server
  accepts any client that can reach it, and a warning is issued.

- allow_other_hostname:

  Accept connections addressed to a hostname other than the one in `uri`
  (default `FALSE`).

- disable_ssl:

  Serve over plain HTTP instead of HTTPS (default `FALSE`). Only
  appropriate on a trusted network.

## Value

The server `uri`, invisibly.

## See also

[`quack_stop()`](https://tgerke.github.io/ducklake-r/reference/quack_stop.md),
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md)

Other quack:
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md),
[`detach_quack()`](https://tgerke.github.io/ducklake-r/reference/detach_quack.md),
[`install_quack()`](https://tgerke.github.io/ducklake-r/reference/install_quack.md),
[`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md),
[`quack_stop()`](https://tgerke.github.io/ducklake-r/reference/quack_stop.md)

## Examples

``` r
if (FALSE) { # \dontrun{
attach_ducklake("trial", lake_path = "~/lakes/trial")
quack_serve(token = "super_secret")
# ... colleagues connect with attach_quack() ...
quack_stop()
} # }
```
