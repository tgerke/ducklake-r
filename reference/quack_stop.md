# Stop a Quack server

Stops a Quack server started with
[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md).

## Usage

``` r
quack_stop(uri = "quack:localhost")
```

## Arguments

- uri:

  Address the server is listening on (default `"quack:localhost"`).

## Value

`TRUE`, invisibly.

## See also

[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md)

Other quack:
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md),
[`detach_quack()`](https://tgerke.github.io/ducklake-r/reference/detach_quack.md),
[`install_quack()`](https://tgerke.github.io/ducklake-r/reference/install_quack.md),
[`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md),
[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md)

## Examples

``` r
if (FALSE) { # \dontrun{
quack_stop()
} # }
```
