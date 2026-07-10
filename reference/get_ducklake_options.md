# List the options set on a DuckLake

Reads the configuration options recorded in the metadata catalog,
including their scope (global, schema, or table).

## Usage

``` r
get_ducklake_options(ducklake_name = NULL)
```

## Arguments

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

A data frame with one row per option setting, including `option_name`,
`value`, `scope` (`GLOBAL`, `SCHEMA`, or `TABLE`), and `scope_entry`.
Options left at their defaults are not listed.

## See also

[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md)

Other options:
[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md)

## Examples

``` r
if (FALSE) { # \dontrun{
get_ducklake_options()
} # }
```
