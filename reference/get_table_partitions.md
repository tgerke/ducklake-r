# List the partitioning keys of tables in a lake

Reads the current partitioning keys from the DuckLake metadata catalog.

## Usage

``` r
get_table_partitions(table_name = NULL, ducklake_name = NULL)
```

## Arguments

- table_name:

  Optional table name to filter to a single table, optionally qualified
  as `"schema.table"`.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

A data frame with one row per partition key: `schema_name`,
`table_name`, `partition_key_index`, `column_name`, and `transform`
(e.g. `"identity"`, `"year"`, or `"bucket(4)"`). Zero rows when nothing
is partitioned.

## See also

[`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md)

Other partitioning:
[`reset_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/reset_table_partitioning.md),
[`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md)

## Examples

``` r
lake_dir <- tempfile("getpart_lake_")
dir.create(lake_dir)
attach_ducklake("getpart_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

set_table_partitioning("cars", "cyl")
#> Table "cars" is now partitioned by "cyl".
#> ℹ Only newly written data is partitioned; existing files keep their layout.

# All partitioned tables in the lake
get_table_partitions()
#>   schema_name table_name partition_key_index column_name transform
#> 1        main       cars                   0         cyl  identity

# Keys for one table
get_table_partitions("cars")
#>   schema_name table_name partition_key_index column_name transform
#> 1        main       cars                   0         cyl  identity

detach_ducklake("getpart_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
