# Check that the active DuckDB engine supports Quack

Check that the active DuckDB engine supports Quack

## Usage

``` r
check_quack_version(conn = NULL)
```

## Arguments

- conn:

  Optional DuckDB connection. Defaults to the ducklake connection.

## Value

Invisibly, `TRUE`. Aborts when the engine is too old.
