# Install the Quack extension

Installs the Quack DuckDB extension, which provides the `quack:`
client-server protocol. Quack is a core extension from DuckDB 1.5.3
onward, so it is autoloaded the first time it is used. Installing it
ahead of time is useful on machines that have no internet access at
query time.

## Usage

``` r
install_quack(load = TRUE)
```

## Arguments

- load:

  If `TRUE` (the default), load the extension after installing it.

## See also

[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md),
[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md)

Other quack:
[`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md),
[`detach_quack()`](https://tgerke.github.io/ducklake-r/reference/detach_quack.md),
[`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md),
[`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md),
[`quack_stop()`](https://tgerke.github.io/ducklake-r/reference/quack_stop.md)

## Examples

``` r
if (FALSE) { # \dontrun{
install_quack()
} # }
```
