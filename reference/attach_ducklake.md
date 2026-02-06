# Create or attach a ducklake

This function is a wrapper for the ducklake
[ATTACH](https://ducklake.select/docs/stable/duckdb/usage/connecting)
command. It will create a new DuckDB-backed DuckLake if the specified
name does not exist, or connect to the existing DuckLake if it does
exist. The connection is stored in the package environment and can be
closed with detach_ducklake().

## Usage

``` r
attach_ducklake(ducklake_name, lake_path = NULL)
```

## Arguments

- ducklake_name:

  Name for the ducklake file, as in `ducklake:{ducklake_name}.ducklake`

- lake_path:

  Optional directory path for the ducklake. If specified, both the
  ducklake database file and Parquet data files will be stored in this
  location. If not specified, the ducklake is created in the current
  working directory with data files in `{ducklake_name}.ducklake.files`.

## See also

[`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md)
to close the connection
