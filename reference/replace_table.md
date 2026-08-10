# Replace a table with modified data and create a new snapshot

Replace a table with modified data and create a new snapshot

## Usage

``` r
replace_table(.data, table_name, .quiet = TRUE)
```

## Arguments

- .data:

  A dplyr query object (tbl_lazy) with transformations

- table_name:

  Table name to replace

- .quiet:

  Logical, whether to suppress messages (default TRUE)

## Value

Invisibly returns NULL

## Details

This function is designed for schema changes or bulk transformations
that should create a new versioned snapshot. It:

1.  Collects the transformed data

2.  Drops the existing table

3.  Creates a new table with the updated schema/data

The drop and create run atomically: when no transaction is open,
`replace_table()` wraps them in one of its own, so a failed create never
leaves the table dropped. Wrap the call in
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
(or
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)/[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md))
when you want to record an author and commit message on the snapshot, or
to group the replacement with other changes.

**When to use replace_table():**

- **Bulk transformations** - a dplyr pipeline that recomputes, reshapes,
  or filters most of the table

**When to reach elsewhere:**

- **Schema-only changes** -
  [`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md),
  [`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md),
  [`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md),
  and
  [`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)
  alter the table in place; nothing is collected or rewritten

- **Derived columns** -
  [`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)
  followed by a
  [`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html)
  pipeline through
  [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
  fills the new column with an in-database UPDATE

- **Targeted row changes** -
  [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
  [`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md),
  or
  [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
  modify only the affected rows

Both paths create a snapshot: replace_table() via DROP + CREATE, and
ducklake_exec() via the in-place UPDATE/DELETE it runs, so either way
the change is available for time travel.

## See also

Other table operations:
[`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md),
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
if (FALSE) { # \dontrun{
# Add new derived columns (atomic on its own; creates a new snapshot)
get_ducklake_table("adsl") |>
  mutate(
    AGE65FL = if_else(AGE >= 65, "Y", "N"),
    AGECAT = case_when(
      AGE < 65 ~ "<65",
      AGE >= 65 & AGE < 75 ~ "65-74",
      AGE >= 75 ~ ">=75"
    )
  ) |>
  replace_table("adsl")

# Wrap in with_transaction() to record audit metadata on the snapshot
with_transaction(
  get_ducklake_table("adsl") |>
    select(-AGE65FL, -AGECAT) |>
    replace_table("adsl"),
  author = "Data Engineer",
  commit_message = "Drop derived age columns"
)
} # }
```
