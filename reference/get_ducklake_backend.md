# Get the current catalog backend type

Get the current catalog backend type

## Usage

``` r
get_ducklake_backend()
```

## Value

One of `"duckdb"`, `"postgres"`, `"sqlite"`, or `"mysql"`. Defaults to
`"duckdb"` when no backend has been set.
