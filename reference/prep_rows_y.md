# Prepare the `y` argument for a rows\_\* operation

Local data frames are converted to an inline query on the same
connection as `x` via
[`dbplyr::copy_inline()`](https://dbplyr.tidyverse.org/reference/copy_inline.html).
Unlike dplyr's `copy = TRUE` path, this creates no temporary table and
starts no transaction of its own, so rows\_\* calls work inside
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
(DuckDB does not support nested transactions).

## Usage

``` r
prep_rows_y(x, y)
```

## Arguments

- x:

  Target lazy table

- y:

  Data frame or lazy table
