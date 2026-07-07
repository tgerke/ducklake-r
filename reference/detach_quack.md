# Disconnect from a remote Quack server

Detaches a remote catalog previously attached with
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md).
The DuckDB connection itself stays alive.

## Usage

``` r
detach_quack(quack_name = NULL)
```

## Arguments

- quack_name:

  Name of the remote catalog to detach. If `NULL`, nothing is detached.

## See also

[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md)

Other quack:
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md),
[`install_quack()`](https://tgerke.github.io/ducklake-r/reference/install_quack.md),
[`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md),
[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md),
[`quack_stop()`](https://tgerke.github.io/ducklake-r/reference/quack_stop.md)

## Examples

``` r
if (FALSE) { # \dontrun{
detach_quack("team")
} # }
```
