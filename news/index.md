# Changelog

## ducklake 0.1.0

Initial release of ducklake, an R package for versioned data lake
infrastructure built on DuckDB and DuckLake.

### Features

#### Core Table Operations

- [`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md) -
  Create new tables in the data lake
- [`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md) -
  Retrieve tables as tibbles
- [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md) -
  Replace entire table contents with versioning

#### Row-Level Operations

- [`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md) -
  Insert new rows with automatic versioning
- [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md) -
  Update existing rows with audit trail
- [`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md) -
  Delete rows while maintaining history

#### ACID Transactions

- [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md) -
  Execute code blocks within transactions
- [`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md),
  [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md),
  [`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md) -
  Manual transaction control
- Full ACID compliance for data integrity

#### Time Travel

- [`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md) -
  Query table state at specific timestamps
- [`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md) -
  Retrieve specific table versions
- [`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md) -
  View complete version history
- [`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md) -
  Roll back to previous versions

#### Metadata and Audit Trail

- [`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md) -
  Access comprehensive metadata
- [`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md) -
  Add author, commit messages, and tags
- Complete lineage tracking for all data changes

#### Connection Management

- [`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md) -
  Install/update DuckLake extension
- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md) -
  Initialize data lake connections
- [`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md) -
  Clean up connections
- [`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md),
  [`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md) -
  Manage active connections

#### Query Execution

- [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md) -
  Execute SQL with automatic assignment handling
- [`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md) -
  Preview translated SQL queries
- [`extract_assignments_from_sql()`](https://tgerke.github.io/ducklake-r/reference/extract_assignments_from_sql.md) -
  Parse SQL table assignments

#### Backup and Maintenance

- [`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md) -
  Create incremental backups
- Support for local and remote backup locations

### Vignettes

- **Getting Started** - Introduction to ducklake workflows
- **Clinical Trial Data Lake** - Industry-specific use case
- **Modifying Tables** - Comprehensive guide to row operations
- **Working with Transactions** - ACID transaction patterns
- **Time Travel Queries** - Historical data access
- **Storage and Backup Management** - Data persistence strategies

### Lifecycle

This package is currently in **experimental** status. The API may change
as we gather feedback from early users, but core functionality is stable
and ready for pilot projects.
