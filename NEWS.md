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
* `get_ducklake_connection()`, `set_ducklake_connection()` - Manage active connections

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
