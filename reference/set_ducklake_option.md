# Set a DuckLake option

Sets a DuckLake configuration option, either lake-wide or scoped to a
schema or table. Options are persisted in the metadata catalog, so they
survive detach/attach cycles and apply to every client of the lake.

## Usage

``` r
set_ducklake_option(
  option,
  value,
  table_name = NULL,
  schema_name = NULL,
  ducklake_name = NULL
)
```

## Arguments

- option:

  Name of the option, e.g. `"parquet_compression"`,
  `"target_file_size"`, `"sort_on_insert"`, or
  `"data_inlining_row_limit"`. See
  <https://ducklake.select/docs/stable/duckdb/usage/configuration> for
  the full list.

- value:

  The value to set. Logicals are rendered as `true`/`false`, numbers as
  numeric literals, and everything else as a quoted string.

- table_name:

  Optional table name to scope the option to one table.

- schema_name:

  Optional schema name to scope the option to one schema (or, together
  with `table_name`, to qualify the table).

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

Invisibly returns `NULL`.

## Details

Table-scoped settings override schema-scoped ones, which override the
lake-wide default. Runs `CALL <lake>.set_option(...)`.

Commonly tuned options include `parquet_compression` (default
`"snappy"`; `"zstd"` trades write speed for smaller files),
`target_file_size` (default `"512MB"`), `sort_on_insert` (default
`TRUE`; see
[`set_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/set_table_sorting.md)),
and `require_commit_message` (default `FALSE`).

## See also

[`get_ducklake_options()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_options.md),
[`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)

Other options:
[`get_ducklake_options()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_options.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Smaller files at some write cost, lake-wide
set_ducklake_option("parquet_compression", "zstd")

# Make every snapshot carry a commit message
set_ducklake_option("require_commit_message", TRUE)

# Skip one table during compaction
set_ducklake_option("auto_compact", FALSE, table_name = "audit_log")
} # }
```
