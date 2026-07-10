# Remove the sort order from a table

Clears a table's declared sort order so newly written data files are no
longer sorted. Existing files are unaffected.

## Usage

``` r
reset_table_sorting(table_name)
```

## Arguments

- table_name:

  The name of the table.

## Value

Invisibly returns `NULL`.

## See also

[`set_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/set_table_sorting.md)

Other sorting:
[`set_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/set_table_sorting.md)

## Examples

``` r
if (FALSE) { # \dontrun{
reset_table_sorting("events")
} # }
```
