# ducklake 0.3.0

## DuckLake v1.0 Specification Alignment

This release aligns the package with the
[DuckLake v1.0 stable specification](https://ducklake.select/docs/stable/specification/introduction),
which requires DuckDB v1.5.2+ (compatible with duckdb R package >= 1.5.1).

### Breaking Changes

* DuckDB version requirement bumped from 1.3.0 to **1.5.1** (duckdb R package)
  / **1.5.2** (DuckDB engine/CLI) to match DuckLake v1.0.
  `install_ducklake()` now enforces this at the engine level.

* `commit_transaction()` and `with_transaction()` now use the official
  `CALL ducklake.set_commit_message()` API to set commit metadata
  **within** the transaction before `COMMIT`, consistent with the v1.0
  specification. `set_snapshot_metadata()` retroactively updates the
  `ducklake_snapshot_changes` metadata table directly.

# ducklake 0.2.0

## Multi-Backend Catalog Support

DuckLake now supports PostgreSQL, SQLite, and MySQL as catalog backends in
addition to DuckDB (#15, @stefanlinner). This aligns with the
[DuckLake 1.0 specification](https://ducklake.select/docs/stable/specification/introduction)
and enables concurrent multi-client access when using PostgreSQL or SQLite.

### New Features

* `attach_ducklake()` gains `backend`, `catalog_connection_string`, `read_only`,
  and `override_data_path` parameters for multi-backend support.
* `install_ducklake()` gains a `backend` parameter to pre-install backend
  extensions (e.g., `install_ducklake(backend = "postgres")`).
* New `get_ducklake_backend()` returns the active catalog backend type.
* `detach_ducklake()` gains a `shutdown` parameter. By default it now performs a
  soft detach (SQL `DETACH` + `USE memory;`) instead of shutting down the
  connection, allowing backend switching within a session.
* `backup_ducklake()` is now backend-aware: file-based backends (DuckDB, SQLite)
  get catalog + data copied; PostgreSQL/MySQL get data only with guidance to use
  `pg_dump`/`mysqldump`. Also fixes a pre-existing bug where catalog backups
  were silently 0 bytes due to DuckDB holding file locks during `file.copy()`.

### Breaking Changes

* `attach_ducklake()` now **requires** `lake_path` (previously optional).
* `set_ducklake_connection()` has been removed. The package now exclusively uses
  duckplyr's singleton DuckDB connection.
* `detach_ducklake()` no longer shuts down the DuckDB connection by default.
  Pass `shutdown = TRUE` for the previous behaviour.

### Internal

* Schema qualifier logic updated throughout (`get_metadata_table()`,
  `time_travel.R`, `transactions.R`) to handle PostgreSQL/MySQL backends that
  don't use the `.main.` schema prefix.
* New internal helpers: `build_attach_sql()`, `ensure_extensions()`,
  `shutdown_and_reset_singleton()`.

---

# ducklake 0.1.0

Initial release of ducklake, an R package for versioned data lake infrastructure built on DuckDB and DuckLake.

## Features

### Core Table Operations
* `create_table()` - Create new tables in the data lake
* `get_ducklake_table()` - Retrieve tables as tibbles
* `replace_table()` - Replace entire table contents with versioning

### Row-Level Operations
* `rows_insert()` - Insert new rows with automatic versioning
* `rows_update()` - Update existing rows with audit trail
* `rows_delete()` - Delete rows while maintaining history

### ACID Transactions
* `with_transaction()` - Execute code blocks within transactions
* `begin_transaction()`, `commit_transaction()`, `rollback_transaction()` - Manual transaction control
* Full ACID compliance for data integrity

### Time Travel
* `get_ducklake_table_asof()` - Query table state at specific timestamps
* `get_ducklake_table_version()` - Retrieve specific table versions
* `list_table_snapshots()` - View complete version history
* `restore_table_version()` - Roll back to previous versions

### Metadata and Audit Trail
* `get_metadata_table()` - Access comprehensive metadata
* `set_snapshot_metadata()` - Add author, commit messages, and tags
* Complete lineage tracking for all data changes

### Connection Management
* `install_ducklake()` - Install/update DuckLake extension
* `attach_ducklake()` - Initialize data lake connections
* `detach_ducklake()` - Clean up connections
* `get_ducklake_connection()` - Retrieve the active DuckDB connection

### Query Execution
* `ducklake_exec()` - Execute SQL with automatic assignment handling
* `show_ducklake_query()` - Preview translated SQL queries
* `extract_assignments_from_sql()` - Parse SQL table assignments

### Backup and Maintenance
* `backup_ducklake()` - Create incremental backups
* Support for local and remote backup locations

## Vignettes

* **Getting Started** - Introduction to ducklake workflows
* **Clinical Trial Data Lake** - Industry-specific use case
* **Modifying Tables** - Comprehensive guide to row operations
* **Working with Transactions** - ACID transaction patterns
* **Time Travel Queries** - Historical data access
* **Storage and Backup Management** - Data persistence strategies

## Lifecycle

This package is currently in **experimental** status. The API may change as we gather feedback from early users, but core functionality is stable and ready for pilot projects.
