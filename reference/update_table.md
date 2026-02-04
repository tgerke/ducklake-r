# Convert a dplyr query to DuckLake SQL operations

Convert a dplyr query to DuckLake SQL operations

## Usage

``` r
update_table(.data, table_name, .quiet = FALSE)
```

## Arguments

- .data:

  A dplyr query object (tbl_lazy)

- table_name:

  Table name (required when using update_table)

- .quiet:

  Logical, whether to suppress debug output (default FALSE for backward
  compatibility)

## Value

A SQL statement string
