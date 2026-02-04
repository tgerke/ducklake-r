# Create or attach a ducklake

This function is a wrapper for the ducklake
[ATTACH](https://ducklake.select/docs/stable/duckdb/usage/connecting)
command. It will create a new DuckDB-backed DuckLake if the specified
name does not exist, or connect to the existing DuckLake if it does
exist. The connection is stored in the package environment and can be
closed with detach_ducklake().

## Usage

``` r
attach_ducklake(ducklake_name, data_path = NULL)
```

## Arguments

- ducklake_name:

  Name for the ducklake file, as in `ducklake:{ducklake_name}.ducklake`

- data_path:

  Optional directory where Parquet files are stored. If not specified,
  uses the default folder `{ducklake_name}.ducklake.files` in the same
  directory as the DuckLake itself.

## See also

[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md)
to close the connection
