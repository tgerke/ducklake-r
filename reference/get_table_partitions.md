# List the partitioning keys of tables in a lake

Reads the current partitioning keys from the DuckLake metadata catalog.

## Usage

``` r
get_table_partitions(table_name = NULL, ducklake_name = NULL)
```

## Arguments

- table_name:

  Optional table name to filter to a single table.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

A data frame with one row per partition key: `table_name`,
`partition_key_index`, `column_name`, and `transform` (e.g. `"identity"`
or `"year"`). Zero rows when nothing is partitioned.

## See also

[`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md)

Other partitioning:
[`reset_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/reset_table_partitioning.md),
[`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# All partitioned tables in the lake
get_table_partitions()

# Keys for one table
get_table_partitions("events")
} # }
```
