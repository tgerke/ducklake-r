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
[`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md),
[`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md),
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
[`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
[`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md),
[`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)

## Examples

``` r
lake_dir <- tempfile("replace_lake_")
dir.create(lake_dir)
attach_ducklake("replace_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

# Add new derived columns (atomic on its own; creates a new snapshot)
get_ducklake_table("cars") |>
  dplyr::mutate(
    thirsty = dplyr::if_else(mpg < 20, "Y", "N"),
    mpg_band = dplyr::case_when(
      mpg < 15 ~ "<15",
      mpg < 25 ~ "15-24",
      TRUE ~ ">=25"
    )
  ) |>
  replace_table("cars")

# Wrap in with_transaction() to record audit metadata on the snapshot
with_transaction(
  get_ducklake_table("cars") |>
    dplyr::select(-thirsty, -mpg_band) |>
    replace_table("cars"),
  author = "Data Engineer",
  commit_message = "Drop derived columns"
)
#> Transaction started.
#> Transaction committed.

detach_ducklake("replace_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
