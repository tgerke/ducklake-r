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

  Optional table name to scope the option to one table, optionally
  qualified as `"schema.table"`.

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

The options DuckLake 1.0 persists, with their defaults:

|  |  |  |
|----|----|----|
| Option | Default | What it controls |
| `auto_compact` | `true` | Whether maintenance calls made without a table argument include the table |
| `data_inlining_row_limit` | `10` | Rows below which an insert or delete is stored in the catalog instead of a file (see [`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)) |
| `delete_older_than` | unset | How long a released file waits before [`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md) and checkpoints delete it |
| `expire_older_than` | unset | How old a snapshot must be before checkpoints expire it |
| `encrypted` | `false` | Encrypt the Parquet files written to the data path (set at creation; see `attach_ducklake(encrypted = )`) |
| `hive_file_pattern` | `true` | Write partitioned data in Hive-style directories |
| `parquet_compression` | `snappy` | Codec: `uncompressed`, `snappy`, `gzip`, `zstd`, `brotli`, `lz4`, or `lz4_raw` |
| `parquet_compression_level` | `3` | Level for codecs that have one |
| `parquet_row_group_size` | `122880` | Rows per row group |
| `parquet_row_group_size_bytes` | unset | Bytes per row group, as an alternative to rows |
| `parquet_version` | `1` | Parquet format version, `1` or `2` |
| `per_thread_output` | `false` | One output file per thread during a parallel insert |
| `require_commit_message` | `false` | Refuse to commit a snapshot without a commit message |
| `rewrite_delete_threshold` | `0.95` | Deleted fraction of a file above which [`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md) rewrites it |
| `sort_on_insert` | `true` | Sort inserted rows by the table's sort keys (see [`set_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/set_table_sorting.md)) |
| `target_file_size` | `512MB` | Target data file size for inserts and compaction |
| `write_deletion_vectors` | `false` | Write Iceberg V3 deletion vectors instead of positional delete files |

`created_by`, `data_path`, and `version` also appear in
[`get_ducklake_options()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_options.md)
but describe the lake rather than configure it. Retention settings
(`expire_older_than`, `delete_older_than`) take interval strings such as
`"90 days"`.

## See also

[`get_ducklake_options()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_options.md),
[`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)

Other options:
[`get_ducklake_options()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_options.md)

## Examples

``` r
lake_dir <- tempfile("setopt_lake_")
dir.create(lake_dir)
attach_ducklake("setopt_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

# Smaller files at some write cost, lake-wide
set_ducklake_option("parquet_compression", "zstd")
#> Option "parquet_compression" set to "zstd" for lake "setopt_lake".

# Skip one table during compaction
set_ducklake_option("auto_compact", FALSE, table_name = "cars")
#> Option "auto_compact" set to FALSE for table "cars".

# Make every snapshot carry a commit message (constrains later writes)
set_ducklake_option("require_commit_message", TRUE)
#> Option "require_commit_message" set to TRUE for lake "setopt_lake".

detach_ducklake("setopt_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
