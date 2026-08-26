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
lake_dir <- tempfile("unsort_lake_")
dir.create(lake_dir)
attach_ducklake("unsort_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

set_table_sorting("cars", "mpg")
#> Table "cars" is now sorted by "mpg".
#> ℹ Only newly written data is sorted; existing files keep their layout until
#>   compaction.
reset_table_sorting("cars")
#> Sort order removed from table "cars".

detach_ducklake("unsort_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
