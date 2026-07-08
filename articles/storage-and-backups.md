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

1.  **Catalog (Metadata)**: A database that stores all metadata about
    your tables, snapshots, transactions, and data file locations. By
    default this is a DuckDB file, but DuckLake also supports
    PostgreSQL, SQLite, or MySQL as catalog backends (see
    [`?attach_ducklake`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)).
    The catalog is typically small but critically important.

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

# PostgreSQL catalog with S3 data - multi-client, scalable
attach_ducklake(
  ducklake_name = "shared_lake",
  backend = "postgres",
  catalog_connection_string = "dbname=ducklake_catalog host=localhost",
  lake_path = "s3://my-bucket/ducklake/data"
)

# SQLite catalog - lightweight multi-client option
attach_ducklake(
  ducklake_name = "team_lake",
  backend = "sqlite",
  catalog_connection_string = "~/data/metadata.sqlite",
  lake_path = "~/data/parquet_files"
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
lake_dir <- file.path(vignette_temp_dir, "storage_demo")
dir.create(lake_dir, showWarnings = FALSE, recursive = TRUE)

# Install ducklake extension
install_ducklake()
#> Installed ducklake extension.

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
#> Transaction started.
#> Transaction committed.

with_transaction(
  get_ducklake_table("cars") |>
    mutate(hp_per_cyl = hp / cyl) |>
    replace_table("cars"),
  author = "Demo User",
  commit_message = "Add hp_per_cyl metric"
)
#> Transaction started.
#> Transaction committed.

with_transaction(
  get_ducklake_table("cars") |>
    mutate(mpg_adjusted = if_else(cyl == 4, mpg * 1.1, mpg)) |>
    replace_table("cars"),
  author = "Demo User",
  commit_message = "Add adjusted MPG for 4-cylinder cars"
)
#> Transaction started.
#> Transaction committed.
```

### Catalog Files

The catalog is a single database file containing all metadata:

``` r

dir_tree(lake_dir)
#> /tmp/RtmpOfaW9I/storage_backups_vignette/storage_demo
#> ├── demo_lake.ducklake
#> ├── demo_lake.ducklake.wal
#> └── main
#>     └── cars
#>         ├── ducklake-019f4394-6a7f-7f31-9c7e-046d2df48aa8.parquet
#>         ├── ducklake-019f4394-6b5c-710a-b505-54651bf0e988.parquet
#>         └── ducklake-019f4394-6bdc-7267-84e6-7dd9d77866bb.parquet
```

The catalog files (`demo_lake.ducklake` and `.wal`) contain all metadata
about tables, snapshots, and transactions.

### Storage (Data) Files

Data files are stored in Parquet format in a structured directory:

``` r

# Data files are organized by schema and table
main_dir <- file.path(lake_dir, "main")

dir_tree(main_dir, recurse = 2)
#> /tmp/RtmpOfaW9I/storage_backups_vignette/storage_demo/main
#> └── cars
#>     ├── ducklake-019f4394-6a7f-7f31-9c7e-046d2df48aa8.parquet
#>     ├── ducklake-019f4394-6b5c-710a-b505-54651bf0e988.parquet
#>     └── ducklake-019f4394-6bdc-7267-84e6-7dd9d77866bb.parquet
  
# Get details about parquet files
parquet_files <- dir_ls(main_dir, recurse = TRUE, regexp = "\\.parquet$")
for (f in parquet_files) {
  cat(sprintf("  %s (%s bytes)\n", 
              path_file(f), 
              file.size(f)))
}
#>   ducklake-019f4394-6a7f-7f31-9c7e-046d2df48aa8.parquet (2307 bytes)
#>   ducklake-019f4394-6b5c-710a-b505-54651bf0e988.parquet (2501 bytes)
#>   ducklake-019f4394-6bdc-7267-84e6-7dd9d77866bb.parquet (2724 bytes)
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
#> 1           1 Demo User                         Initial load
#> 2           2 Demo User                Add hp_per_cyl metric
#> 3           3 Demo User Add adjusted MPG for 4-cylinder cars
```

The key insight is that **DuckLake never modifies or deletes existing
Parquet files**. Each change creates new files, preserving the complete
history for time travel queries.

## Backup Strategies

### Backing Up the Catalog

The catalog is the most critical component—it maps snapshots to data
files. Regular backups are essential.

#### Simple File Copy

For local databases, the simplest backup is a file copy. One rule
matters: **release the file locks first**. DuckDB holds the catalog file
open while a lake is attached, and copying a live catalog produces a
corrupt (or, on Windows, unreadable) backup. Detach with
`shutdown = TRUE`, copy, then re-attach — or skip the manual steps
entirely and use
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md),
which does exactly this dance for you:

``` r

# Create backup directory
backup_dir <- file.path(lake_dir, "backups")
dir.create(backup_dir, showWarnings = FALSE)

# Release file locks before copying the catalog
detach_ducklake("demo_lake", shutdown = TRUE)

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
#> /tmp/RtmpOfaW9I/storage_backups_vignette/storage_demo/backups
#> ├── demo_lake.ducklake
#> └── main
#>     └── cars
#>         ├── ducklake-019f4394-6a7f-7f31-9c7e-046d2df48aa8.parquet
#>         ├── ducklake-019f4394-6b5c-710a-b505-54651bf0e988.parquet
#>         └── ducklake-019f4394-6bdc-7267-84e6-7dd9d77866bb.parquet

# To work with the backup, attach it. override_data_path is needed because
# the catalog remembers the original data location, which the backup no
# longer matches.
attach_ducklake(
  ducklake_name = "demo_lake",
  lake_path = backup_dir,
  override_data_path = TRUE
)

# Verify you're working with the backup
list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-07-08 21:13:47              1
#> 2           2 2026-07-08 21:13:47              2
#> 3           3 2026-07-08 21:13:48              3
#>                                                                 changes
#> 1                    tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 2, 3
#>      author                       commit_message commit_extra_info
#> 1 Demo User                         Initial load              <NA>
#> 2 Demo User                Add hp_per_cyl metric              <NA>
#> 3 Demo User Add adjusted MPG for 4-cylinder cars              <NA>

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
  lake_path = "s3://original-bucket/data"
)

# After recovery from replicated bucket
attach_ducklake(
  ducklake_name = "prod_lake",
  lake_path = "s3://backup-bucket/data",
  override_data_path = TRUE
)
```

## Recovery Procedures

### Recovering from Catalog Backup

If your catalog is corrupted or lost:

``` r

# Restore from backup by copying the backup file
# (backup_dir here is a directory created earlier, e.g. by backup_ducklake())
file.copy(
  from = file.path(backup_dir, "demo_lake.ducklake"),
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

## Routine Maintenance

A lake that sees regular writes accumulates small Parquet files (one per
insert) and old snapshots whose files can no longer be reclaimed until
the snapshots are expired. The one-stop command is
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
which flushes inlined data, merges small files, expires old snapshots,
and cleans up unreferenced files in a single call. For finer control,
each step has its own function:

``` r

# Compact small adjacent Parquet files into larger ones
merge_adjacent_files()

# Preview a retention policy, then apply it
expire_snapshots(older_than = Sys.time() - 30 * 24 * 60 * 60, dry_run = TRUE)
expire_snapshots(older_than = Sys.time() - 30 * 24 * 60 * 60)

# Expired snapshots only *schedule* file deletion; this reclaims the storage
cleanup_old_files(cleanup_all = TRUE)

# Rewrite data files whose rows have mostly been deleted
rewrite_data_files(delete_threshold = 0.5)

# Remove untracked files from the data path -- always dry-run this one first
delete_orphaned_files(dry_run = TRUE, cleanup_all = TRUE)
```

The typical cycle is merge, then expire, then clean up: merging and
expiring both mark files as unreferenced, and
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md)
deletes them. Expiring a snapshot gives up time travel to it, so choose
`older_than` to match how far back you need to audit or restore.

One task lives outside DuckLake itself: the catalog database. If you use
a PostgreSQL or SQLite catalog, occasionally run `VACUUM` there with
that database’s own tooling so metadata queries stay fast. The default
DuckDB-file catalog does not need this.

## Maintenance Considerations

When planning backups, coordinate with maintenance operations:

- **Compaction** (merging adjacent files): Run before backups to ensure
  consistent file layout
- **Cleanup** (removing obsolete files): Run before backups to avoid
  backing up unnecessary files

``` r

# Recommended backup sequence

# 1. Run maintenance operations (if needed)
merge_adjacent_files()
expire_snapshots(older_than = Sys.time() - 30 * 24 * 60 * 60)
cleanup_old_files(cleanup_all = TRUE)

# 2. Ensure all transactions are committed
# (no pending work)

# 3. Release file locks before copying the catalog
detach_ducklake("demo_lake", shutdown = TRUE)

# 4. Back up catalog
dir.create(file.path(lake_dir, "backups"), showWarnings = FALSE)
file.copy(
  from = file.path(lake_dir, "demo_lake.ducklake"),
  to = file.path(lake_dir, "backups",
                 paste0("backup_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".ducklake"))
)

# 5. Back up data files
dir_copy(
  path = file.path(lake_dir, "main"),
  new_path = file.path(lake_dir, "backups", "main_latest")
)

# 6. Re-attach and continue working
attach_ducklake("demo_lake", lake_path = lake_dir)
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
#> Catalog backed up successfully.
#> Data files backed up successfully (1 directory).
#> Backup completed:
#> /tmp/RtmpOfaW9I/storage_backups_vignette/storage_demo/backups/backup_20260708_211349

# The function returns the backup directory path
print(backup_dir)
#> [1] "/tmp/RtmpOfaW9I/storage_backups_vignette/storage_demo/backups/backup_20260708_211349"
```

The
[`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md)
function: - Creates a timestamped backup directory - Copies the catalog
database file (releasing file locks first) - Copies the data files from
every schema directory in the lake - Returns the backup directory path
for reference

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
