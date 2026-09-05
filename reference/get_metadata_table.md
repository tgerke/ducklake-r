# Get a DuckLake metadata table

DuckLake keeps all of its bookkeeping – snapshots, table schemas, data
file locations, and more – in ordinary tables inside the catalog
database. This function gives you a lazy reference to any of them, which
is handy for auditing and for understanding how your lake evolves.

## Usage

``` r
get_metadata_table(tbl_name, ducklake_name = NULL)
```

## Arguments

- tbl_name:

  Character string, name of the metadata table to retrieve (e.g.,
  `"ducklake_snapshot"`).

- ducklake_name:

  Character string, name of the ducklake database (optional, defaults to
  the currently active ducklake).

## Value

A lazy table that works with dplyr verbs.

## Details

Commonly useful tables include `ducklake_snapshot` (one row per
snapshot), `ducklake_table` (registered tables), and
`ducklake_data_file` (the Parquet files backing each table). The full
list is in the [DuckLake
specification](https://ducklake.select/docs/stable/specification/introduction).

## See also

[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md)
for a friendlier view of snapshot history.

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`create_schema()`](https://tgerke.github.io/ducklake-r/reference/create_schema.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md),
[`drop_schema()`](https://tgerke.github.io/ducklake-r/reference/drop_schema.md),
[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
lake_dir <- tempfile("meta_lake_")
dir.create(lake_dir)
attach_ducklake("meta_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

# Every snapshot ever taken
get_metadata_table("ducklake_snapshot") |> dplyr::collect()
#> # A tibble: 2 × 5
#>   snapshot_id snapshot_time       schema_version next_catalog_id next_file_id
#>         <dbl> <dttm>                       <dbl>           <dbl>        <dbl>
#> 1           0 2026-09-05 01:58:27              0               1            0
#> 2           1 2026-09-05 01:58:27              1               2            1

# Which Parquet files back the lake?
get_metadata_table("ducklake_data_file") |>
  dplyr::select(data_file_id, path) |>
  dplyr::collect()
#> # A tibble: 1 × 2
#>   data_file_id path                                                 
#>          <dbl> <chr>                                                
#> 1            0 ducklake-01a06f49-e14e-7ae4-a492-7aab3a6cb97f.parquet

detach_ducklake("meta_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
