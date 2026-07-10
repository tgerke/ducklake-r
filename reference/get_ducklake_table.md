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

  Character string, name of the table to retrieve.

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
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
if (FALSE) { # \dontrun{
attach_ducklake("my_lake", lake_path = "~/data/lake")
create_table(mtcars, "cars")

# Query lazily with dplyr, then collect
get_ducklake_table("cars") |>
  dplyr::filter(cyl > 4) |>
  dplyr::summarise(avg_mpg = mean(mpg), .by = cyl) |>
  dplyr::collect()
} # }
```
