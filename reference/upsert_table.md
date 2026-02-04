# Upsert data from a dplyr query into a DuckLake table

Performs a MERGE operation: updates existing rows and inserts new ones
based on matching keys. This is the pipeline-based version of
rows_upsert() for use with dplyr queries.

## Usage

``` r
upsert_table(.data, table_name = NULL, by, .quiet = TRUE)
```

## Arguments

- .data:

  A dplyr query object (tbl_lazy) with the source data

- table_name:

  The target table name. If not provided, will be extracted from the
  table attribute (set by get_ducklake_table())

- by:

  Character vector of column names to match on (merge keys)

- .quiet:

  Logical, whether to suppress debug output (default TRUE)

## Value

The result from executing the MERGE statement

## Details

This is the pipeline-based approach to upserts, ideal when transforming
data with dplyr verbs. For upserting data.frames directly, see
[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md).

This function generates a DuckDB INSERT ... ON CONFLICT statement (which
provides MERGE/UPSERT functionality). Rows are matched based on the
columns specified in `by`. If a match is found, the row is updated; if
not, a new row is inserted.

**Note:** This function requires that the table has a PRIMARY KEY or
UNIQUE constraint on the columns specified in `by`. If your table
doesn't have these constraints, use
[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)
instead.

## See also

[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)
for the data.frame approach to upserts

## Examples

``` r
if (FALSE) { # \dontrun{
# Upsert data from a computed query
get_ducklake_table("staging_table") |>
  mutate(processed = TRUE) |>
  upsert_table("target_table", by = "id")

# Upsert with table name inferred
get_ducklake_table("my_table") |>
  filter(status == "active") |>
  mutate(last_updated = Sys.time()) |>
  upsert_table(by = c("id", "version"))
} # }
```
