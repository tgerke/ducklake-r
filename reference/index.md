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
  : Get the DuckDB connection used by ducklake
- [`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)
  : Use your own DuckDB connection with ducklake
- [`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md)
  : Get the catalog backend type of an attached lake

## Table Operations

Create and query tables

- [`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
  : Create a DuckLake table
- [`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md)
  : Get a DuckLake table
- [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
  : Replace a table with modified data and create a new snapshot

## Row Operations

Modify table rows with dplyr-style functions

- [`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md)
  : Insert rows into a DuckLake table
- [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md)
  : Update rows in a DuckLake table
- [`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md)
  : Delete rows from a DuckLake table

## Query Execution

Execute and preview SQL queries

- [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
  : Execute DuckLake operations from dplyr queries
- [`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)
  : Show the SQL that would be executed by ducklake operations

## Transactions

ACID transaction support

- [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
  : Execute code within a transaction
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
- [`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md)
  : Get the changes made to a table between two snapshots
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

## Data Inlining

Configure and manage data inlining for streaming workloads

- [`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)
  : Set the data inlining row limit
- [`get_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/get_inlining_row_limit.md)
  : Get the current data inlining row limit
- [`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md)
  : Flush inlined data to Parquet files
- [`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)
  : Run a DuckLake checkpoint

## Partitioning

Manage table partition keys for file pruning

- [`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md)
  : Set partitioning keys for a table
- [`reset_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/reset_table_partitioning.md)
  : Remove partitioning keys from a table
- [`get_table_partitions()`](https://tgerke.github.io/ducklake-r/reference/get_table_partitions.md)
  : List the partitioning keys of tables in a lake

## Backup and Maintenance

Backup, compaction, and storage reclamation

- [`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md)
  : Create a DuckLake backup
- [`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md)
  : Expire old snapshots
- [`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md)
  : Merge adjacent Parquet files
- [`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md)
  : Delete files scheduled for removal
- [`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md)
  : Delete orphaned files
- [`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)
  : Rewrite data files with many deletes

## Quack remote access

Connect to and serve DuckLake over the Quack protocol

- [`install_quack()`](https://tgerke.github.io/ducklake-r/reference/install_quack.md)
  : Install the Quack extension
- [`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md)
  : Connect to a remote Quack server
- [`detach_quack()`](https://tgerke.github.io/ducklake-r/reference/detach_quack.md)
  : Disconnect from a remote Quack server
- [`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md)
  : Run a one-off query against a remote Quack server
- [`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md)
  : Serve the current session over Quack
- [`quack_stop()`](https://tgerke.github.io/ducklake-r/reference/quack_stop.md)
  : Stop a Quack server
