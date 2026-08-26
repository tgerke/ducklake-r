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
lake_dir <- tempfile("unpart_lake_")
dir.create(lake_dir)
attach_ducklake("unpart_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

set_table_partitioning("cars", "cyl")
#> Table "cars" is now partitioned by "cyl".
#> ℹ Only newly written data is partitioned; existing files keep their layout.
reset_table_partitioning("cars")
#> Partitioning removed from table "cars".

detach_ducklake("unpart_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
