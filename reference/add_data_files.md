# Register existing Parquet files with a DuckLake table

Adds Parquet files that already exist on disk (or object storage) to a
DuckLake table without copying or rewriting them. This is the migration
path for data that is already in Parquet: the files are recorded in the
catalog in place, and one snapshot is created per file added.

## Usage

``` r
add_data_files(
  table_name,
  files,
  schema_name = NULL,
  allow_missing = FALSE,
  ignore_extra_columns = FALSE,
  ducklake_name = NULL
)
```

## Arguments

- table_name:

  The table to add the files to. It must already exist with a schema
  compatible with the files (see `allow_missing` and
  `ignore_extra_columns` for the two permitted mismatches).

- files:

  Character vector of Parquet file paths or URIs.

- schema_name:

  Optional schema containing the table (defaults to the lake's `main`
  schema).

- allow_missing:

  If `TRUE`, files may lack columns that exist in the table; missing
  columns read as the column's initial default. Default `FALSE`.

- ignore_extra_columns:

  If `TRUE`, files may contain columns that the table does not have; the
  extra columns are inaccessible. Default `FALSE`.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

Invisibly returns the character vector of files added.

## Details

Runs `CALL ducklake_add_data_files(...)` once per file. Ownership of
each file transfers to DuckLake: compaction (e.g.
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md))
may later rewrite and delete it, so do not add files that something else
still relies on.

## See also

[`list_ducklake_files()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_files.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)

Other table operations:
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Bring an existing Parquet extract into the lake without copying it
create_table(data.frame(id = integer(), value = numeric()), "readings")
add_data_files("readings", "extracts/readings_2026.parquet")

# Several files at once, tolerating a column the table doesn't have
add_data_files(
  "readings",
  c("extracts/jan.parquet", "extracts/feb.parquet"),
  ignore_extra_columns = TRUE
)
} # }
```
