# Package index

## Setup and Connection

Initialize and manage DuckLake connections

- [`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)
  : Install the ducklake extension to duckdb
- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  : Create or attach a ducklake
- [`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md)
  : Detach from a ducklake
- [`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md)
  : Get the current DuckLake connection
- [`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)
  : Set the DuckLake connection

## Table Operations

Create and query tables

- [`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
  : Create a DuckLake table
- [`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md)
  : Get a DuckLake table
- [`update_table()`](https://tgerke.github.io/ducklake-r/reference/update_table.md)
  : Convert a dplyr query to DuckLake SQL operations
- [`upsert_table()`](https://tgerke.github.io/ducklake-r/reference/upsert_table.md)
  : Upsert data from a dplyr query into a DuckLake table

## Row Operations

Modify table rows with dplyr-style functions

- [`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md)
  : Insert rows into a DuckLake table
- [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md)
  : Update rows in a DuckLake table
- [`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)
  : Upsert rows in a DuckLake table
- [`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md)
  : Delete rows from a DuckLake table
- [`rows_patch()`](https://tgerke.github.io/ducklake-r/reference/rows_patch.md)
  : Patch rows in a DuckLake table

## Query Execution

Execute and preview SQL queries

- [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
  : Execute DuckLake operations from dplyr queries
- [`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)
  : Show the SQL that would be executed by ducklake operations
- [`extract_assignments_from_sql()`](https://tgerke.github.io/ducklake-r/reference/extract_assignments_from_sql.md)
  : Extract column assignments from SQL SELECT statement

## Transactions

ACID transaction support

- [`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)
  : Begin a transaction
- [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
  : Commit a transaction
- [`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md)
  : Rollback a transaction

## Time Travel

Query and restore historical data

- [`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md)
  : Query a table at a specific timestamp (time travel)
- [`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md)
  : Query a table at a specific version/snapshot
- [`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md)
  : List available snapshots for a table
- [`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)
  : Restore a table to a previous version

## Metadata

Access metadata and snapshots

- [`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md)
  : Get a DuckLake metadata table
- [`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
  : Set metadata for the most recent snapshot
