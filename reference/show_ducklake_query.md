# Show the SQL that would be executed by ducklake operations

This function shows the SQL that would be generated and executed by
ducklake. This is useful for debugging and understanding what SQL is
being sent to DuckDB.

## Usage

``` r
show_ducklake_query(.data, table_name = NULL)
```

## Arguments

- .data:

  A dplyr query object (tbl_lazy)

- table_name:

  The target table name for the operation. If not provided, will be
  extracted from the table attribute (set by get_ducklake_table())

## Value

The first argument, invisibly (following show_query convention)

## Examples

``` r
if (FALSE) { # \dontrun{
# Show SQL for an update operation (table name inferred)
get_ducklake_table("my_table") |>
  mutate(status = "updated") |>
  show_ducklake_query()
} # }
```
