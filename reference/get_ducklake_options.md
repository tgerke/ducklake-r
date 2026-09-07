# List the options set on a DuckLake

Reads the configuration options recorded in the metadata catalog,
including their scope (global, schema, or table).

## Usage

``` r
get_ducklake_options(ducklake_name = NULL)
```

## Arguments

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

A data frame with one row per option setting, including `option_name`,
`value`, `scope` (`GLOBAL`, `SCHEMA`, or `TABLE`), and `scope_entry`.
Options left at their defaults are not listed.

## See also

[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md)

Other options:
[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md)

## Examples

``` r
lake_dir <- tempfile("getopt_lake_")
dir.create(lake_dir)
attach_ducklake("getopt_lake", lake_path = lake_dir)

set_ducklake_option("parquet_compression", "zstd")
#> Option "parquet_compression" set to "zstd" for lake "getopt_lake".
get_ducklake_options()
#>           option_name
#> 1          created_by
#> 2           data_path
#> 3           encrypted
#> 4 parquet_compression
#> 5             version
#>                                                                                        description
#> 1                                                                  Tool used to write the DuckLake
#> 2                                                                               Path to data files
#> 3                                 Whether or not to encrypt Parquet files written to the data path
#> 4 Compression algorithm for Parquet files (uncompressed, snappy, gzip, zstd, brotli, lz4, lz4_raw)
#> 5                                                                          DuckLake format version
#>                                       value  scope scope_entry
#> 1                         DuckDB d8cdaa33fd GLOBAL        <NA>
#> 2 /tmp/Rtmpr0HcoA/getopt_lake_1acd69a2392e/ GLOBAL        <NA>
#> 3                                     false GLOBAL        <NA>
#> 4                                      zstd GLOBAL        <NA>
#> 5                                       1.0 GLOBAL        <NA>

detach_ducklake("getopt_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
