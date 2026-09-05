# List the sort keys of tables in a lake

Reads the current sort order from the DuckLake metadata catalog, the
counterpart of
[`get_table_partitions()`](https://tgerke.github.io/ducklake-r/reference/get_table_partitions.md)
for
[`set_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/set_table_sorting.md).

## Usage

``` r
get_table_sorting(table_name = NULL, ducklake_name = NULL)
```

## Arguments

- table_name:

  Optional table name to filter to a single table.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

A data frame with one row per sort key: `table_name`, `sort_key_index`,
`expression`, `sort_direction` (`"ASC"` or `"DESC"`), and `null_order`
(`"NULLS_FIRST"` or `"NULLS_LAST"`). Zero rows when nothing is sorted.

## See also

[`set_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/set_table_sorting.md),
[`get_table_partitions()`](https://tgerke.github.io/ducklake-r/reference/get_table_partitions.md)

Other sorting:
[`reset_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/reset_table_sorting.md),
[`set_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/set_table_sorting.md)

## Examples

``` r
lake_dir <- tempfile("getsort_lake_")
dir.create(lake_dir)
attach_ducklake("getsort_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

set_table_sorting("cars", c("cyl ASC", "mpg DESC"))
#> Table "cars" is now sorted by "cyl ASC" and "mpg DESC".
#> ℹ Only newly written data is sorted; existing files keep their layout until
#>   compaction.

# All sorted tables in the lake
get_table_sorting()
#>   table_name sort_key_index expression sort_direction null_order
#> 1       cars              0        cyl            ASC NULLS_LAST
#> 2       cars              1        mpg           DESC NULLS_LAST

# Keys for one table
get_table_sorting("cars")
#>   table_name sort_key_index expression sort_direction null_order
#> 1       cars              0        cyl            ASC NULLS_LAST
#> 2       cars              1        mpg           DESC NULLS_LAST

detach_ducklake("getsort_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
