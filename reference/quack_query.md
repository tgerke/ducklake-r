# Run a one-off query against a remote Quack server

Sends a single SQL query to a Quack server and returns the result as a
data.frame. Unlike
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md),
this does not attach the remote catalog, so it is a quick way to pull a
result without changing the session state.

## Usage

``` r
quack_query(uri, query, token = NULL, disable_ssl = FALSE)
```

## Arguments

- uri:

  Address of the Quack server, for example `"quack:localhost"`.

- query:

  A SQL query string to run on the server.

- token:

  Authentication token expected by the server. If `NULL`, a Quack secret
  is used if one exists.

- disable_ssl:

  Connect over plain HTTP instead of HTTPS (default `FALSE`).

## Value

A data.frame with the query result.

## See also

[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md)

Other quack:
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md),
[`detach_quack()`](https://tgerke.github.io/ducklake-r/reference/detach_quack.md),
[`install_quack()`](https://tgerke.github.io/ducklake-r/reference/install_quack.md),
[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md),
[`quack_stop()`](https://tgerke.github.io/ducklake-r/reference/quack_stop.md)

## Examples

``` r
if (FALSE) { # \dontrun{
quack_query(
  "quack:data.example.org",
  "SELECT count(*) FROM adsl",
  token = "super_secret"
)
} # }
```
