# Get a DuckLake table

Returns a lazy reference to a table in the attached DuckLake. Like
[`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html),
nothing is read until you
[`collect()`](https://dplyr.tidyverse.org/reference/compute.html): build
up your
[`filter()`](https://dplyr.tidyverse.org/reference/filter.html)/[`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html)/[`summarise()`](https://dplyr.tidyverse.org/reference/summarise.html)
pipeline first and DuckDB executes it as a single query, only pulling
the rows you asked for into R.

## Usage

``` r
get_ducklake_table(tbl_name)
```

## Arguments

- tbl_name:

  Character string, name of the table to retrieve. A table outside the
  `main` schema is named `"schema.table"`.

## Value

A lazy table (class `tbl_ducklake`) that works with dplyr verbs. The
table name is stored in the `ducklake_table_name` attribute.

## See also

[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
to create tables,
[`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md)
and
[`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md)
for time-travel reads.

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`create_schema()`](https://tgerke.github.io/ducklake-r/reference/create_schema.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md),
[`drop_schema()`](https://tgerke.github.io/ducklake-r/reference/drop_schema.md),
[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
lake_dir <- tempfile("cars_lake_")
dir.create(lake_dir)
attach_ducklake("cars_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

# Query lazily with dplyr, then collect
get_ducklake_table("cars") |>
  dplyr::filter(cyl > 4) |>
  dplyr::summarise(avg_mpg = mean(mpg), .by = cyl) |>
  dplyr::collect()
#> Warning: Missing values are always removed in SQL aggregation functions.
#> Use `na.rm = TRUE` to silence this warning
#> This warning is displayed once every 8 hours.
#> # A tibble: 2 × 2
#>     cyl avg_mpg
#>   <dbl>   <dbl>
#> 1     6    19.7
#> 2     8    15.1

detach_ducklake("cars_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
