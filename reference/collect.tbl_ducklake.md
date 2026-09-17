# Reattach stored column comments as label attributes on collect

Completes the label round trip:
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
stores haven/labelled `label` attributes as column comments, and
collecting the table brings them back, so gtsummary, gt, and friends
display them as usual. Columns renamed or derived in the pipeline simply
come back unlabelled. A time-travel read
([`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md),
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md))
gets the labels in force at its snapshot.

## Usage

``` r
# S3 method for class 'tbl_ducklake'
collect(x, ...)
```

## Arguments

- x:

  A `tbl_ducklake` lazy table.

- ...:

  Passed on to
  [`dplyr::collect()`](https://dplyr.tidyverse.org/reference/compute.html).

## Value

A tibble, with `label` attributes on columns that have stored comments.
