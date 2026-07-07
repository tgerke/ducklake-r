# ducklake 0.4.0

This release focuses on production hardiness: self-contained connection
management, working detach/restore, SQL identifier safety, Quack remote
access, and a documentation overhaul.

## Quack remote protocol support

Added support for Quack, DuckDB's client-server protocol, which became a core
extension in DuckDB 1.5.3 (#20, @JavOrraca). A DuckLake served by one DuckDB
instance can now be queried and modified by other R sessions over the network.
For concurrent access this is a lighter-weight option than a PostgreSQL or
SQLite catalog, since the whole setup stays inside DuckDB and DuckLake.

* `attach_quack()` connects to a remote Quack server and attaches it as a catalog in the current session.
* `detach_quack()` disconnects from a remote Quack server.
* `install_quack()` installs the Quack DuckDB extension.
* `quack_query()` runs a one-off query against a remote Quack server and returns a data.frame.
* `quack_serve()` serves the current session, including an attached DuckLake, to other clients over Quack.
* `quack_stop()` stops a running Quack server.

## Production hardening

### New Features

* `attach_ducklake()` gains an `encrypted` argument: pass `encrypted = TRUE`
  to have DuckLake encrypt the Parquet files it writes (#18). Note that the
  encryption keys are stored in the catalog database, so protect the catalog.
* `restore_table_version()` now works. It previously generated a
  `RESTORE TABLE` statement that does not exist in DuckLake and failed on
  every call. It now recreates the table from a time-travel read inside a
  transaction, recording the restore as a new snapshot so history is
  preserved.
* `get_ducklake_backend()` gains a `ducklake_name` argument and tracks each
  attached lake separately, so sessions with several lakes on different
  catalog backends resolve backend-specific behaviour correctly.

### Bug Fixes

* `detach_ducklake()` now actually detaches. Previously the `DETACH` ran
  while the lake was still the session's current database, which DuckDB
  refuses, and the error was silently swallowed -- the lake stayed attached.
  The session now switches back to the connection's own catalog first.
  Relatedly, restoring a backup to a new location requires
  `override_data_path = TRUE` (as documented); the storage vignette example
  has been corrected.
* Table names, lake names, and file paths are now quoted or validated before
  being interpolated into SQL (`DBI::dbQuoteIdentifier()` and friends), so
  names with spaces or quotes no longer produce malformed statements.
* `rows_insert()`, `rows_update()`, and `rows_delete()` now also dispatch as
  S3 methods on tables returned by `get_ducklake_table()`. Previously, if
  dplyr was loaded *after* ducklake, dplyr's generics masked ducklake's
  wrappers and calls failed with `conflict = "error"` complaints; load order
  no longer matters.
* `backup_ducklake()` backs up every schema directory, not just `main`.
* `create_table()` now converts factor columns to character (with a message)
  instead of failing with "unsupported type ENUM" -- DuckLake does not
  support DuckDB's ENUM type, which is what factors become.
* The internal dplyr-to-SQL translation in `ducklake_exec()` no longer uses
  `sink()` (which could leak diverted output on error), and now refuses
  queries with subqueries or multiple `WHERE` clauses instead of generating
  incorrect SQL.

## Connection management is now self-contained

ducklake now creates and manages its own DuckDB connection instead of
reaching into duckplyr's unexported internals. This removes the package's
last `:::` calls and the duckplyr dependency entirely.

### Breaking Changes

* duckplyr is no longer a dependency. If you relied on ducklake sharing
  duckplyr's default connection, register a connection explicitly with the
  new `set_ducklake_connection()`.

### New Features

* `set_ducklake_connection()` (returning by popular demand, now safer):
  point ducklake at any DuckDB connection you manage — for example one
  shared with other DBI tools. Connections you supply are never closed by
  ducklake; only its own automatically created connection is shut down by
  `detach_ducklake(shutdown = TRUE)` and at session exit.

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
