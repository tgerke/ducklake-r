# Changelog

## ducklake 0.3.0

### DuckLake v1.0 Specification Alignment

This release aligns the package with the [DuckLake v1.0 stable
specification](https://ducklake.select/docs/stable/specification/introduction),
which requires DuckDB v1.5.2+ (compatible with duckdb R package \>=
1.5.1).

#### Breaking Changes

- DuckDB version requirement bumped from 1.3.0 to **1.5.1** (duckdb R
  package) / **1.5.2** (DuckDB engine/CLI) to match DuckLake v1.0.
  [`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)
  now enforces this at the engine level.

- [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
  and
  [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
  now use the official `CALL ducklake.set_commit_message()` API to set
  commit metadata **within** the transaction before `COMMIT`, consistent
  with the v1.0 specification.
  [`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
  retroactively updates the `ducklake_snapshot_changes` metadata table
  directly.

## ducklake 0.2.0

### Multi-Backend Catalog Support

DuckLake now supports PostgreSQL, SQLite, and MySQL as catalog backends
in addition to DuckDB
([\#15](https://github.com/tgerke/ducklake-r/issues/15),
[@stefanlinner](https://github.com/stefanlinner)). This aligns with the
[DuckLake 1.0
specification](https://ducklake.select/docs/stable/specification/introduction)
and enables concurrent multi-client access when using PostgreSQL or
SQLite.

#### New Features

- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  gains `backend`, `catalog_connection_string`, `read_only`, and
  `override_data_path` parameters for multi-backend support.
- [`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)
  gains a `backend` parameter to pre-install backend extensions (e.g.,
  `install_ducklake(backend = "postgres")`).
- New
  [`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md)
  returns the active catalog backend type.
- [`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md)
  gains a `shutdown` parameter. By default it now performs a soft detach
  (SQL `DETACH` + `USE memory;`) instead of shutting down the
  connection, allowing backend switching within a session.
- [`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md)
  is now backend-aware: file-based backends (DuckDB, SQLite) get
  catalog + data copied; PostgreSQL/MySQL get data only with guidance to
  use `pg_dump`/`mysqldump`. Also fixes a pre-existing bug where catalog
  backups were silently 0 bytes due to DuckDB holding file locks during
  [`file.copy()`](https://rdrr.io/r/base/files.html).

#### Breaking Changes

- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  now **requires** `lake_path` (previously optional).
- `set_ducklake_connection()` has been removed. The package now
  exclusively uses duckplyr’s singleton DuckDB connection.
- [`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md)
  no longer shuts down the DuckDB connection by default. Pass
  `shutdown = TRUE` for the previous behaviour.

#### Internal

- Schema qualifier logic updated throughout
  ([`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
  `time_travel.R`, `transactions.R`) to handle PostgreSQL/MySQL backends
  that don’t use the `.main.` schema prefix.
- New internal helpers:
  [`build_attach_sql()`](https://tgerke.github.io/ducklake-r/reference/build_attach_sql.md),
  [`ensure_extensions()`](https://tgerke.github.io/ducklake-r/reference/ensure_extensions.md),
  [`shutdown_and_reset_singleton()`](https://tgerke.github.io/ducklake-r/reference/shutdown_and_reset_singleton.md).

------------------------------------------------------------------------

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
- [`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md) -
  Retrieve the active DuckDB connection

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
