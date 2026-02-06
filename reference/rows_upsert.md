# Upsert rows in a DuckLake table

A wrapper around dplyr::rows_upsert() with in_place = TRUE as the
default, since DuckLake is designed for in-place modifications.
Optionally adds snapshot metadata after the operation completes.

## Usage

``` r
rows_upsert(
  x,
  y,
  by = NULL,
  copy = TRUE,
  in_place = TRUE,
  author = NULL,
  commit_message = NULL,
  commit_extra_info = NULL,
  ...
)
```

## Arguments

- x:

  Target table (from get_ducklake_table())

- y:

  Data frame with rows to upsert (update existing, insert new)

- by:

  Column(s) to match on

- copy:

  Whether to copy y to the same source as x (default TRUE)

- in_place:

  Whether to modify the table in place (default TRUE for DuckLake)

- author:

  Optional author name to associate with the snapshot

- commit_message:

  Optional commit message describing the changes

- commit_extra_info:

  Optional extra information about the commit

- ...:

  Additional arguments passed to dplyr::rows_upsert()

## Value

The updated table

## Details

This function performs an upsert operation: updates existing rows and
inserts new ones. Rows are matched using the columns specified in `by`.

If `author`, `commit_message`, or `commit_extra_info` are provided, they
will be added to the snapshot metadata after the upsert completes.

## See also

[`upsert_table()`](https://tgerke.github.io/ducklake-r/reference/upsert_table.md)
for pipeline-based upserts using dplyr transformations

## Examples

``` r
if (FALSE) { # \dontrun{
# Basic upsert
rows_upsert(
  get_ducklake_table("my_table"),
  data.frame(id = c(1, 99), value = c("updated", "new")),
  by = "id"
)

# Upsert with metadata
rows_upsert(
  get_ducklake_table("my_table"),
  data.frame(id = c(1, 99), value = c("updated", "new")),
  by = "id",
  author = "Data Team",
  commit_message = "Update and add records"
)
} # }
```
