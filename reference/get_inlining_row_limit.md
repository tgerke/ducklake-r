# Get the current data inlining row limit

Returns the effective data inlining row limit. When no table- or
schema-level override is configured, the global DuckDB default is
returned.

## Usage

``` r
get_inlining_row_limit(
  table_name = NULL,
  schema_name = NULL,
  ducklake_name = NULL
)
```

## Arguments

- table_name:

  Optional table name to query the table-level override.

- schema_name:

  Optional schema name to query the schema-level override.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

An integer: the effective inlining row limit.

## See also

[`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Global default
get_inlining_row_limit()

# Table-specific limit
get_inlining_row_limit(table_name = "readings")
} # }
```
