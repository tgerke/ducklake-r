# Set partitioning keys for a table

Declares how newly written data files for a table should be split up.
Partitioning lets DuckLake prune files during query planning, which can
speed up filtered reads on large tables considerably.

## Usage

``` r
set_table_partitioning(table_name, partition_by)
```

## Arguments

- table_name:

  The name of the table to partition.

- partition_by:

  Character vector of partition expressions. Each entry must be one of:

  - a column name, e.g. `"region"` (identity transform)

  - `"year(col)"`, `"month(col)"`, `"day(col)"`, or `"hour(col)"` for
    timestamp columns

  - `"bucket(n, col)"` for hash bucketing into `n` buckets

## Value

Invisibly returns `NULL`.

## Details

Partitioning only affects data written *after* the keys are set;
previously written files keep their layout. To re-partition existing
data, rewrite the table (e.g. with
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md))
after setting the keys.

Runs `ALTER TABLE ... SET PARTITIONED BY (...)`. The expressions are
validated against the transforms DuckLake supports before any SQL is
built.

## See also

[`reset_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/reset_table_partitioning.md),
[`get_table_partitions()`](https://tgerke.github.io/ducklake-r/reference/get_table_partitions.md)

Other partitioning:
[`get_table_partitions()`](https://tgerke.github.io/ducklake-r/reference/get_table_partitions.md),
[`reset_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/reset_table_partitioning.md)

## Examples

``` r
lake_dir <- tempfile("part_lake_")
dir.create(lake_dir)
attach_ducklake("part_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

# Plain column partitioning
set_table_partitioning("cars", "cyl")
#> Table "cars" is now partitioned by "cyl".
#> ℹ Only newly written data is partitioned; existing files keep their layout.

# Compound key
set_table_partitioning("cars", c("gear", "cyl"))
#> Table "cars" is now partitioned by "gear" and "cyl".
#> ℹ Only newly written data is partitioned; existing files keep their layout.

# Timestamp columns can be split by year(), month(), day(), or hour()

detach_ducklake("part_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
