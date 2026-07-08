# Remove partitioning keys from a table

Clears a table's partitioning keys so newly written data files are no
longer split along them. Existing files are unaffected.

## Usage

``` r
reset_table_partitioning(table_name)
```

## Arguments

- table_name:

  The name of the table.

## Value

Invisibly returns `NULL`.

## See also

[`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md),
[`get_table_partitions()`](https://tgerke.github.io/ducklake-r/reference/get_table_partitions.md)

Other partitioning:
[`get_table_partitions()`](https://tgerke.github.io/ducklake-r/reference/get_table_partitions.md),
[`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md)

## Examples

``` r
if (FALSE) { # \dontrun{
reset_table_partitioning("events")
} # }
```
