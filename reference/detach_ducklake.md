# Detach from a ducklake

Closes the DuckDB connection and detaches from the current DuckLake.

## Usage

``` r
detach_ducklake(ducklake_name = NULL)
```

## Arguments

- ducklake_name:

  Optional name of the ducklake to detach. If not provided, closes the
  current connection.

## Examples

``` r
if (FALSE) { # \dontrun{
attach_ducklake("my_ducklake")
# ... do work ...
detach_ducklake("my_ducklake")
} # }
```
