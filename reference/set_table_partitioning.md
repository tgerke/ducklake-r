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
if (FALSE) { # \dontrun{
# Partition new files by year and month of the event timestamp
set_table_partitioning("events", c("year(event_time)", "month(event_time)"))

# Plain column partitioning
set_table_partitioning("sales", "region")

# Hash user ids into 8 buckets, then split by month
set_table_partitioning("visits", c("bucket(8, user_id)", "month(ts)"))
} # }
```
