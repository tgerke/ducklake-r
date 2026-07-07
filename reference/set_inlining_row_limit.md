# Set the data inlining row limit

Controls the threshold below which DuckLake stores small inserts and
deletes directly in the catalog database instead of writing Parquet
files. This avoids the "small files problem" common in streaming or
frequent-update workloads.

## Usage

``` r
set_inlining_row_limit(
  limit,
  table_name = NULL,
  schema_name = NULL,
  ducklake_name = NULL
)
```

## Arguments

- limit:

  Integer. The maximum number of rows that will be inlined. Set to `0`
  to disable inlining entirely.

- table_name:

  Optional table name. When provided the limit is persisted for that
  table in the DuckLake metadata (takes priority over the global
  setting).

- schema_name:

  Optional schema name. When provided (without `table_name`) the limit
  is persisted for all tables in that schema.

- ducklake_name:

  Optional name of the attached DuckLake catalog. Required when setting
  a table- or schema-level override. If `NULL`, the current database is
  used.

## Value

Invisibly returns `NULL`.

## Details

The limit can be set at three levels (highest priority first):

1.  **Table-level** – persisted in the DuckLake metadata for a specific
    table

2.  **Schema-level** – persisted for all tables in a schema

3.  **Global (DuckDB setting)** – applies to all DuckLake connections

Data inlining is enabled by default in DuckLake v1.0 with a threshold of
10 rows. Any insert or delete affecting fewer rows than the limit is
written to an inlined table inside the catalog instead of creating a
Parquet file.

For streaming or high-frequency-insert workloads, increase the limit
(e.g., 50 or 100). For workloads that always write large batches, the
default is fine or you can disable inlining with `limit = 0`.

Use
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md)
or
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)
to materialise inlined data to Parquet when ready.

## See also

[`get_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/get_inlining_row_limit.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)

Other data inlining:
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`get_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/get_inlining_row_limit.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Change the global default
set_inlining_row_limit(50)

# Override for a specific table
set_inlining_row_limit(100, table_name = "readings")

# Disable inlining globally
set_inlining_row_limit(0)
} # }
```
