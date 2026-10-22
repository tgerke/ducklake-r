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
  disable_ssl = FALSE,
  secret = NULL,
  create_secret_if_not_exists = FALSE,
  ssl_cert_file = NULL,
  ssl_key_file = NULL
)
```

## Arguments

- uri:

  Address to listen on (default `"quack:localhost"`). The default port
  is 9494.

- token:

  Authentication token that clients must supply (at least 4 characters).
  If `NULL`, the server takes the token of the session's default Quack
  secret when there is one (see `secret` and
  `create_secret_if_not_exists`), and otherwise generates a random
  token, which the returned data frame carries in `auth_token`. A server
  is never open: clients always present a token.

- allow_other_hostname:

  Accept connections addressed to a hostname other than the one in `uri`
  (default `FALSE`). A server reachable from other machines belongs
  behind a TLS-terminating reverse proxy.

- disable_ssl:

  Serve over plain HTTP instead of HTTPS (default `FALSE`, which leaves
  the choice to DuckDB: plain HTTP for a local address, HTTPS
  otherwise). Only appropriate on a trusted network.

- secret:

  Optional name of an existing Quack secret whose token the server uses.

- create_secret_if_not_exists:

  When no `token` is given and no default Quack secret exists, store the
  generated token in a persistent default secret, so later servers (and
  clients in this session) reuse it (default `FALSE`).

- ssl_cert_file, ssl_key_file:

  Optional certificate and private key files for serving over HTTPS,
  such as the pair
  [`quack_generate_keys()`](https://tgerke.github.io/ducklake-r/reference/quack_generate_keys.md)
  writes. Without them DuckDB generates a self-signed certificate when
  it serves HTTPS.

## Value

Invisibly, a data frame with one row: `listen_uri`, `listen_url`, and
`auth_token`, the token clients must present.

## See also

[`quack_stop()`](https://tgerke.github.io/ducklake-r/reference/quack_stop.md),
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md),
[`quack_generate_keys()`](https://tgerke.github.io/ducklake-r/reference/quack_generate_keys.md)

Other quack:
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md),
[`detach_quack()`](https://tgerke.github.io/ducklake-r/reference/detach_quack.md),
[`install_quack()`](https://tgerke.github.io/ducklake-r/reference/install_quack.md),
[`quack_generate_keys()`](https://tgerke.github.io/ducklake-r/reference/quack_generate_keys.md),
[`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md),
[`quack_stop()`](https://tgerke.github.io/ducklake-r/reference/quack_stop.md)

## Examples

``` r
if (FALSE) { # \dontrun{
attach_ducklake("trial", lake_path = "path/to/lake")
quack_serve(token = "super_secret")
# ... colleagues connect with attach_quack() ...
quack_stop()

# Let DuckDB pick the token and keep it in a secret for later servers
server <- quack_serve(create_secret_if_not_exists = TRUE)
server$auth_token
} # }
```
