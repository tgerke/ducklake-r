# Working with Transactions

``` r
library(ducklake)
library(dplyr)
library(duckplyr)

# Setup for examples
install_ducklake()
attach_ducklake("my_ducklake")
create_table("nl_train_stations", "https://blobs.duckdb.org/nl_stations.csv")
```

## Transaction support

Group multiple operations together with ACID transactions. This ensures
that either all operations succeed or none do, maintaining data
consistency.

## Basic transaction workflow

``` r
# Check what data we currently have
get_ducklake_table("nl_train_stations") |>
  filter(code %in% c("HT", "ASB")) |>
  select(code, name_short) |>
  collect()
```

``` r
# Start a transaction
begin_transaction()

# Make multiple changes atomically within the transaction
duckplyr::db_exec("UPDATE nl_train_stations SET name_short = 'COMMITTED_CHANGE' WHERE code = 'HT'")
duckplyr::db_exec("UPDATE nl_train_stations SET name_short = 'ALSO_COMMITTED' WHERE code = 'ASB'")

# Commit both changes together
commit_transaction()

# Add author and commit message metadata to the snapshot
set_snapshot_metadata(
  ducklake_name = "my_ducklake",
  author = "Data Team",
  commit_message = "Updated station names for clarity"
)

# Verify the changes were applied
get_ducklake_table("nl_train_stations") |>
  filter(code %in% c("HT", "ASB")) |>
  select(code, name_short) |>
  collect()
```

## View commit history

``` r
# View the recent commit history with metadata
get_metadata_table("ducklake_snapshot_changes", ducklake_name = "my_ducklake") |>
  select(snapshot_id, changes_made, author, commit_message) |>
  collect() |>
  tail(3)
```

## Rolling back transactions

If you need to undo changes before committing:

``` r
# Start another transaction
begin_transaction()

# Make a change we'll roll back
duckplyr::db_exec("UPDATE nl_train_stations SET name_short = 'ROLLBACK_TEST' WHERE code = 'HT'")

# Decide to rollback instead of commit
rollback_transaction()

# Verify the change was NOT applied (should still be "COMMITTED_CHANGE")
get_ducklake_table("nl_train_stations") |>
  filter(code == "HT") |>
  select(code, name_short) |>
  collect()
```

## Key concepts

- **[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)**:
  Start a new transaction
- **[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)**:
  Apply all changes made within the transaction
- **[`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md)**:
  Discard all changes made within the transaction
- **[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)**:
  Add metadata (author, commit message) to snapshots

Transactions are essential for maintaining data integrity when making
multiple related changes.
