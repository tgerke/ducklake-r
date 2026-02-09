# Storage and Backup Management

``` r
library(ducklake)
library(dplyr)
library(fs)
```

## Introduction

Understanding how DuckLake stores and manages your data is crucial for
maintaining a robust data lake. This vignette explains:

- The two-component architecture of DuckLake (catalog and storage)
- What types of files are created and how to inspect them
- Best practices for choosing storage locations
- How to implement backup and recovery strategies

## DuckLake’s Two-Component Architecture

DuckLake separates data management into two distinct components:

1.  **Catalog (Metadata)**: A database file (DuckDB, SQLite, or
    PostgreSQL) that stores all metadata about your tables, snapshots,
    transactions, and data file locations. This is typically small but
    critically important.

2.  **Storage (Data Files)**: A directory containing immutable Parquet
    files that hold your actual data. DuckLake never modifies existing
    files—it only creates new ones.

This separation provides several benefits:

- **Simplified consistency**: Since files are never modified, caching
  and replication are straightforward
- **Flexible storage options**: Store metadata locally and data in the
  cloud, or vice versa
- **Independent backup strategies**: Each component can be backed up
  differently based on your needs

## Storage Options

DuckLake works with any filesystem backend that DuckDB supports,
including:

- **Local files and folders**: Fast access, ideal for single-machine
  workflows
- **Cloud object storage**:
  - AWS S3 (and S3-compatible services like Cloudflare R2, MinIO)
  - Google Cloud Storage
  - Azure Blob Storage
- **Network-attached storage**: NFS, SMB, FUSE-based filesystems

### Storage Patterns

Here are how some common storage patterns may look:

``` r
# Local storage - fastest, but not shared
attach_ducklake(
  ducklake_name = "local_lake",
  lake_path = "~/data/my_ducklake"
)

# Cloud storage - scalable, accessible from anywhere
attach_ducklake(
  ducklake_name = "cloud_lake",
  lake_path = "s3://my-bucket/ducklake",
  data_path = "s3://my-bucket/ducklake/data"
)

# Hybrid approach - metadata local, data in cloud
attach_ducklake(
  ducklake_name = "hybrid_lake",
  lake_path = "~/data/metadata",
  data_path = "s3://my-bucket/data"
)
```

**Key considerations:**

- **Latency vs. accessibility**: Local storage is fast but not
  shareable; cloud storage is accessible but has higher latency
- **Scalability vs. cost**: Object stores scale easily but may charge
  for data transfer
- **Security**: Consider using DuckLake’s encryption features for cloud
  storage

## Inspecting DuckLake Files

Let’s create a sample DuckLake and explore what files it generates:

``` r
# Create a temporary directory for our demo
lake_dir <- file.path(tempdir(), "storage_demo")
dir.create(lake_dir, showWarnings = FALSE, recursive = TRUE)

# Install ducklake extension
install_ducklake()

# Create and populate a DuckLake
attach_ducklake(
  ducklake_name = "demo_lake",
  lake_path = lake_dir
)

# Add some data with transactions
with_transaction(
  create_table(mtcars[1:15, ], "cars"),
  author = "Demo User",
  commit_message = "Initial load"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

with_transaction(
  get_ducklake_table("cars") |>
    mutate(hp_per_cyl = hp / cyl) |>
    replace_table("cars"),
  author = "Demo User",
  commit_message = "Add hp_per_cyl metric"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

with_transaction(
  get_ducklake_table("cars") |>
    mutate(mpg_adjusted = if_else(cyl == 4, mpg * 1.1, mpg)) |>
    replace_table("cars"),
  author = "Demo User",
  commit_message = "Add adjusted MPG for 4-cylinder cars"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated
```

### Catalog Files

The catalog is a single database file containing all metadata:

``` r
dir_tree(lake_dir)
#> /tmp/RtmpwQVvHw/storage_demo
#> ├── demo_lake.ducklake
#> ├── demo_lake.ducklake.wal
#> └── main
#>     └── cars
#>         ├── ducklake-019c43dd-fccf-7564-b356-2f60ec62d33e.parquet
#>         ├── ducklake-019c43dd-fdab-763d-823d-3ac5dbf869b6.parquet
#>         └── ducklake-019c43dd-fdf9-7ffb-940c-34c41acf85f5.parquet
```

The catalog files (`demo_lake.ducklake` and `.wal`) contain all metadata
about tables, snapshots, and transactions.

### Storage (Data) Files

Data files are stored in Parquet format in a structured directory:

``` r
# Data files are organized by schema and table
main_dir <- file.path(lake_dir, "main")

dir_tree(main_dir, recurse = 2)
#> /tmp/RtmpwQVvHw/storage_demo/main
#> └── cars
#>     ├── ducklake-019c43dd-fccf-7564-b356-2f60ec62d33e.parquet
#>     ├── ducklake-019c43dd-fdab-763d-823d-3ac5dbf869b6.parquet
#>     └── ducklake-019c43dd-fdf9-7ffb-940c-34c41acf85f5.parquet
  
# Get details about parquet files
parquet_files <- dir_ls(main_dir, recurse = TRUE, regexp = "\\.parquet$")
for (f in parquet_files) {
  cat(sprintf("  %s (%s bytes)\n", 
              path_file(f), 
              file.size(f)))
}
#>   ducklake-019c43dd-fccf-7564-b356-2f60ec62d33e.parquet (2271 bytes)
#>   ducklake-019c43dd-fdab-763d-823d-3ac5dbf869b6.parquet (2462 bytes)
#>   ducklake-019c43dd-fdf9-7ffb-940c-34c41acf85f5.parquet (2682 bytes)
```

### Understanding File Organization

Each table’s data is organized by schema and table, with each
transaction creating new Parquet files:

``` r
# List all snapshots to see the version history
snapshots <- list_table_snapshots("cars")
snapshots |>
  select(snapshot_id, author, commit_message)
#>   snapshot_id    author                       commit_message
#> 2           1 Demo User                         Initial load
#> 3           2 Demo User                Add hp_per_cyl metric
#> 4           3 Demo User Add adjusted MPG for 4-cylinder cars
```

The key insight is that **DuckLake never modifies or deletes existing
Parquet files**. Each change creates new files, preserving the complete
history for time travel queries.

## Backup Strategies

### Backing Up the Catalog

The catalog is the most critical component—it maps snapshots to data
files. Regular backups are essential.

#### Simple File Copy

For local databases, the simplest backup is a file copy:

``` r
# Create backup directory
backup_dir <- file.path(lake_dir, "backups")
dir.create(backup_dir, showWarnings = FALSE)

# Copy the catalog file to create a backup
file.copy(
  from = file.path(lake_dir, "demo_lake.ducklake"),
  to = file.path(backup_dir, "demo_lake.ducklake")
)
#> [1] TRUE

# Copy the data directory as well
dir_copy(
  path = file.path(lake_dir, "main"),
  new_path = file.path(backup_dir, "main")
)

# Verify the backup was created
dir_tree(backup_dir)
#> /tmp/RtmpwQVvHw/storage_demo/backups
#> ├── demo_lake.ducklake
#> └── main
#>     └── cars
#>         ├── ducklake-019c43dd-fccf-7564-b356-2f60ec62d33e.parquet
#>         ├── ducklake-019c43dd-fdab-763d-823d-3ac5dbf869b6.parquet
#>         └── ducklake-019c43dd-fdf9-7ffb-940c-34c41acf85f5.parquet

# To use the backup, detach the current lake and attach to the backup
# First detach the original
detach_ducklake("demo_lake")

# Attach to the backup location
attach_ducklake(
  ducklake_name = "demo_lake",
  lake_path = backup_dir
)

# Verify you're working with the backup
list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 2           1 2026-02-09 19:25:47              1
#> 3           2 2026-02-09 19:25:47              2
#> 4           3 2026-02-09 19:25:47              3
#>                                                                 changes
#> 2                    tables_created, tables_inserted_into, main.cars, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 4 tables_created, tables_dropped, tables_inserted_into, main.cars, 2, 3
#>      author                       commit_message commit_extra_info
#> 2 Demo User                         Initial load              <NA>
#> 3 Demo User                Add hp_per_cyl metric              <NA>
#> 4 Demo User Add adjusted MPG for 4-cylinder cars              <NA>

# You can switch back to the original by detaching and reattaching
detach_ducklake("demo_lake")
attach_ducklake("demo_lake", lake_path = lake_dir)
```

**Important**: Transactions committed after a backup won’t be tracked
when recovering. The data will exist in the Parquet files, but the
backup will point to an earlier snapshot.

**Best practices:**

- Back up after batch jobs complete
- For streaming/continuous updates, schedule periodic backups
- Consider using [cronR](https://github.com/bnosac/cronR) or
  [taskscheduleR](https://github.com/bnosac/taskscheduleR) for automated
  backups

### Backing Up Storage (Data Files)

Since Parquet files are immutable, backing up storage is
straightforward.

#### Local Storage Backup

``` r
# Use file system tools to copy the entire data directory
backup_data_dir <- file.path(lake_dir, "backups", "main_backup")
dir_copy(
  path = file.path(lake_dir, "main"),
  new_path = backup_data_dir
)
```

#### Cloud Storage Backup

For cloud storage, use provider-specific mechanisms:

**AWS S3:** - Cross-bucket replication (copies to a different bucket
automatically) - AWS Backup service (scheduled backups within the same
bucket) - S3 versioning (keeps previous versions of objects)

**Google Cloud Storage:** - Cross-bucket replication - Backup and DR
service - Object versioning with soft deletes

When using cross-bucket replication, update your data path:

``` r
# Original
attach_ducklake(
  ducklake_name = "prod_lake",
  data_path = "s3://original-bucket/data"
)

# After recovery from replicated bucket
attach_ducklake(
  ducklake_name = "prod_lake",
  data_path = "s3://backup-bucket/data"
)
```

## Recovery Procedures

### Recovering from Catalog Backup

If your catalog is corrupted or lost:

``` r
# Restore from backup by copying the backup file
file.copy(
  from = file.path(lake_dir, "backups", 
                   paste0("demo_lake_backup_", Sys.Date(), ".ducklake")),
  to = file.path(lake_dir, "demo_lake.ducklake"),
  overwrite = TRUE
)

# Reattach to the restored database
attach_ducklake("demo_lake", lake_path = lake_dir)

# Verify recovery by listing snapshots
list_table_snapshots("cars")
```

### Recovering from Data File Loss

If data files are lost but the catalog is intact:

``` r
# Restore data files from backup
dir_copy(
  path = backup_data_dir,
  new_path = file.path(lake_dir, "main"),
  overwrite = TRUE
)

# DuckLake will automatically reconnect to the restored files
# since the catalog maintains the file paths
```

## Maintenance Considerations

When planning backups, coordinate with maintenance operations:

- **Compaction** (merging adjacent files): Run before backups to ensure
  consistent file layout
- **Cleanup** (removing obsolete files): Run before backups to avoid
  backing up unnecessary files

``` r
# Recommended backup sequence

# 1. Run maintenance operations (if needed)
# See maintenance vignettes for details

# 2. Ensure all transactions are committed
# (no pending work)

# 3. Back up catalog
dir.create(file.path(lake_dir, "backups"), showWarnings = FALSE)
file.copy(
  from = file.path(lake_dir, "demo_lake.ducklake"),
  to = file.path(lake_dir, "backups", 
                 paste0("backup_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".ducklake"))
)

# 4. Back up data files
dir_copy(
  path = file.path(lake_dir, "main"),
  new_path = file.path(lake_dir, "backups", "main_latest")
)
```

## Complete Backup Example

DuckLake provides a convenient
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md)
function for creating timestamped backups:

``` r
# Create a complete backup with timestamp
backup_dir <- backup_ducklake(
  ducklake_name = "demo_lake",
  lake_path = lake_dir,
  backup_path = file.path(lake_dir, "backups")
)
#> Catalog backed up successfully
#> Data files backed up successfully
#> Backup completed: /tmp/RtmpwQVvHw/storage_demo/backups/backup_20260209_192548

# The function returns the backup directory path
print(backup_dir)
#> [1] "/tmp/RtmpwQVvHw/storage_demo/backups/backup_20260209_192548"
```

The
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md)
function: - Creates a timestamped backup directory - Copies the catalog
database file - Copies all data files from the main/ directory - Returns
the backup directory path for reference

## Cleanup

``` r
# Detach the demo lake
detach_ducklake("demo_lake")

# Clean up temporary files
unlink(lake_dir, recursive = TRUE)
```

## Summary

Key takeaways for managing DuckLake storage and backups:

1.  **Understand the architecture**: Catalog (metadata) and storage
    (data) are separate components
2.  **Choose storage wisely**: Balance latency, scalability, cost, and
    accessibility
3.  **Files are immutable**: DuckLake never modifies existing Parquet
    files
4.  **Back up regularly**: Catalog backups are critical; back up after
    batch jobs
5.  **Coordinate with maintenance**: Run compaction and cleanup before
    backups
6.  **Test recovery procedures**: Ensure you can actually restore from
    backups

For production systems, consider:

- Automated backup scheduling (using
  [cronR](https://github.com/bnosac/cronR) or
  [taskscheduleR](https://github.com/bnosac/taskscheduleR))
- Multiple backup locations (local and cloud)
- Testing recovery procedures regularly
- Monitoring backup success and storage usage
- Version control for catalog schema changes
