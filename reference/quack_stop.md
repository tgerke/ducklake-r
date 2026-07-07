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

## Examples

``` r
if (FALSE) { # \dontrun{
quack_stop()
} # }
```
