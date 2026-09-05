# Register existing Parquet files with a DuckLake table

Adds Parquet files that already exist on disk (or object storage) to a
DuckLake table without copying or rewriting them. This is the migration
path for data that is already in Parquet: the files are recorded in the
catalog in place.

## Usage

``` r
add_data_files(
  table_name,
  files,
  schema_name = NULL,
  allow_missing = FALSE,
  ignore_extra_columns = FALSE,
  create = FALSE,
  ducklake_name = NULL
)
```

## Arguments

- table_name:

  The table to add the files to, optionally qualified as
  `"schema.table"`. Unless `create = TRUE`, it must already exist with a
  schema compatible with the files (see `allow_missing` and
  `ignore_extra_columns` for the permitted mismatches).

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

- create:

  If `TRUE`, create an empty target table from the registered Parquet
  schema. The table must not already exist. Default `FALSE`.

- ducklake_name:

  Optional name of the attached DuckLake catalog. If `NULL`, the current
  database is used.

## Value

Invisibly returns the character vector of files added.

## Details

Runs `CALL ducklake_add_data_files(...)` once per file. The complete
vector is atomic: outside an existing transaction the function opens
one, so the batch creates one snapshot and any failure rolls back every
registration. Inside
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
the registrations join the caller's snapshot.

With `create = TRUE`, the table schema is read from the complete file
list with `read_parquet()` and created with zero rows before
registration. Neither this path nor registration copies the data or
materializes it in R.

Ownership of each file transfers to DuckLake: compaction (e.g.
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md))
may later rewrite and delete it, so do not add files that something else
still relies on.

## See also

[`list_ducklake_files()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_files.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)

Other table operations:
[`create_schema()`](https://tgerke.github.io/ducklake-r/reference/create_schema.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md),
[`drop_schema()`](https://tgerke.github.io/ducklake-r/reference/drop_schema.md),
[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
lake_dir <- tempfile("addfiles_lake_")
dir.create(lake_dir)
attach_ducklake("addfiles_lake", lake_path = lake_dir)

# Write a couple of Parquet extracts to register. Cast explicitly: DuckDB
# reads a bare 1.5 as DECIMAL, which will not map onto a DOUBLE column.
extract_dir <- tempfile("extracts_")
dir.create(extract_dir)
conn <- get_ducklake_connection()
jan <- file.path(extract_dir, "jan.parquet")
feb <- file.path(extract_dir, "feb.parquet")
DBI::dbExecute(conn, sprintf(
  "COPY (SELECT 1::INTEGER AS id, 1.5::DOUBLE AS value)
   TO '%s' (FORMAT PARQUET);", jan
))
#> [1] 1
DBI::dbExecute(conn, sprintf(
  "COPY (SELECT 2::INTEGER AS id, 2.5::DOUBLE AS value)
   TO '%s' (FORMAT PARQUET);", feb
))
#> [1] 1

# Bring an existing Parquet extract into the lake without copying it
create_table(data.frame(id = integer(), value = numeric()), "readings")
add_data_files("readings", jan)
#> Added 1 file to table "readings".
#> ℹ DuckLake now owns the added file; compaction may rewrite or delete it.

# Register another, tolerating a column the table doesn't have
add_data_files("readings", feb, ignore_extra_columns = TRUE)
#> Added 1 file to table "readings".
#> ℹ DuckLake now owns the added file; compaction may rewrite or delete it.

# Create a new table and register an existing Parquet batch atomically
add_data_files("staging", feb, create = TRUE)
#> Added 1 file to table "staging".
#> ℹ DuckLake now owns the added file; compaction may rewrite or delete it.

unlink(extract_dir, recursive = TRUE)

detach_ducklake("addfiles_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
