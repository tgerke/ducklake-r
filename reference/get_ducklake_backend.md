# Get the catalog backend type of an attached lake

Get the catalog backend type of an attached lake

## Usage

``` r
get_ducklake_backend(ducklake_name = NULL)
```

## Arguments

- ducklake_name:

  Name of the lake to look up. When `NULL` (the default), the lake the
  session is currently `USE`ing is looked up.

## Value

One of `"duckdb"`, `"postgres"`, `"sqlite"`, or `"mysql"`. Defaults to
`"duckdb"` when the lake is unknown.

## See also

Other connection management:
[`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md),
[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md),
[`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
[`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md),
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)

## Examples

``` r
if (FALSE) { # \dontrun{
attach_ducklake("my_lake", lake_path = "~/data/lake")
get_ducklake_backend()
#> [1] "duckdb"

# With several lakes attached, look one up by name
get_ducklake_backend("my_sqlite_lake")
} # }
```
