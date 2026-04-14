# Detach from a ducklake

Detaches the DuckLake database but keeps the DuckDB connection alive by
default. Use `shutdown = TRUE` to also close the connection and release
file locks.

## Usage

``` r
detach_ducklake(ducklake_name = NULL, shutdown = FALSE)
```

## Arguments

- ducklake_name:

  Optional name of the ducklake to detach.

- shutdown:

  If `TRUE`, shut down the DuckDB connection after detaching.

## Examples

``` r
if (FALSE) { # \dontrun{
attach_ducklake("my_ducklake", lake_path = "path/to/lake")
# ... do work ...
detach_ducklake("my_ducklake")

# Full shutdown when completely done
detach_ducklake("my_ducklake", shutdown = TRUE)
} # }
```
