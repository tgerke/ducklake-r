# Generate a self-signed certificate for a Quack server

Writes `server.pem` and `private_key.pem` for
[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md)
to serve HTTPS with, and returns their paths with the certificate's
SHA-256 fingerprint, which clients pass as `ssl_fingerprint` to
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md)
or
[`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md)
so they can trust the self-signed certificate. Existing key files are
not overwritten.

## Usage

``` r
quack_generate_keys(directory = NULL)
```

## Arguments

- directory:

  Directory to write the files into. `NULL` (the default) uses DuckDB's
  own location, `~/.duckdb/extension_data/quack`.

## Value

A data frame with one row: `cert_file`, `key_file`, `fingerprint`, and
`status`.

## See also

[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md)

Other quack:
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md),
[`detach_quack()`](https://tgerke.github.io/ducklake-r/reference/detach_quack.md),
[`install_quack()`](https://tgerke.github.io/ducklake-r/reference/install_quack.md),
[`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md),
[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md),
[`quack_stop()`](https://tgerke.github.io/ducklake-r/reference/quack_stop.md)

## Examples

``` r
if (FALSE) { # \dontrun{
keys <- quack_generate_keys(directory = "~/quack-keys")
quack_serve(
  "quack:0.0.0.0:9494", token = "super_secret",
  allow_other_hostname = TRUE,
  ssl_cert_file = keys$cert_file, ssl_key_file = keys$key_file
)
# Clients pin the certificate:
# attach_quack("team", "quack:host:9494", token = "super_secret",
#              ssl_fingerprint = keys$fingerprint)
} # }
```
