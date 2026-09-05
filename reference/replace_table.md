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

This function is designed for bulk transformations that should create a
new versioned snapshot. A dplyr pipeline on the package's connection
runs inside DuckDB: its result is materialized in DuckDB's temporary
storage (the query may read the table being replaced), the table is
dropped and recreated from it, and the metadata DuckLake keeps against
the table is put back. No rows pass through R. A data frame, or a lazy
table on another connection, is loaded the way
[`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
loads it.

The drop and create run atomically: when no transaction is open,
`replace_table()` wraps them in one of its own, so a failed create never
leaves the table dropped. Wrap the call in
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
(or
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)/[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md))
when you want to record an author and commit message on the snapshot, or
to group the replacement with other changes.

DuckLake gives the replacement a new table id, as it does for any
`DROP` + `CREATE`, and it stores comments, partition keys, sort order,
and table-scoped options against that id. `replace_table()` carries them
over: the table comment, column comments (and so variable labels) for
columns that still exist, partition and sort keys whose columns still
exist (set before the rows are written, so the rewrite itself lands
partitioned and sorted), and options set with
[`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md)
at table scope. DuckLake cannot set options on a table created in the
open transaction, so options are re-set right after the rewrite commits,
as a small follow-up snapshot; inside a transaction you opened yourself
they cannot be re-set at all, and a warning lists the calls to make
after your commit. Earlier snapshots keep the earlier id and stay
reachable by name through time travel.

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
ducklake_exec() via the in-place UPDATE/DELETE/INSERT it runs, so either
way the change is available for time travel.

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

# Partition keys, sort order, comments, and table options survive the rewrite
set_table_partitioning("cars", "cyl")
#> Table "cars" is now partitioned by "cyl".
#> ℹ Only newly written data is partitioned; existing files keep their layout.
get_ducklake_table("cars") |>
  dplyr::filter(mpg > 15) |>
  replace_table("cars")
get_table_partitions("cars")
#>   table_name partition_key_index column_name transform
#> 1       cars                   0         cyl  identity

detach_ducklake("replace_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
