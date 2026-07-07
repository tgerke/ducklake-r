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

## See also

Other table operations:
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Show SQL for an update operation (table name inferred)
get_ducklake_table("my_table") |>
  mutate(status = "updated") |>
  show_ducklake_query()
} # }
```
