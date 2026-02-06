# Create a complete DuckLake backup

Creates a timestamped backup of both the catalog database and data
files. The backup includes the complete state of the DuckLake at the
time of backup, allowing for point-in-time recovery.

## Usage

``` r
backup_ducklake(ducklake_name, lake_path, backup_path)
```

## Arguments

- ducklake_name:

  Name of the attached DuckLake

- lake_path:

  Path to the DuckLake directory containing the catalog file

- backup_path:

  Directory where backups should be stored. A timestamped subdirectory
  will be created within this path.

## Value

Invisibly returns the path to the created backup directory

## Details

The function creates a complete backup by:

1.  Creating a timestamped backup directory

2.  Copying the catalog database file (.ducklake)

3.  Copying all data files from the main/ directory

**Important notes:**

- Transactions committed after a backup won't be tracked when
  recovering. The data will exist in the Parquet files, but the backup
  will point to an earlier snapshot.

- Consider coordinating backups with maintenance operations (compaction
  and cleanup) for optimal storage efficiency.

- For production systems, schedule backups using `{cronR}` or
  `{taskscheduleR}`.

## Examples

``` r
if (FALSE) { # \dontrun{
# Create a DuckLake
lake_dir <- tempfile("my_lake")
dir.create(lake_dir)
attach_ducklake("my_lake", lake_path = lake_dir)

# Add some data
with_transaction(
  create_table(mtcars, "cars"),
  author = "User",
  commit_message = "Initial data"
)

# Create a backup
backup_dir <- backup_ducklake(
  ducklake_name = "my_lake",
  lake_path = lake_dir,
  backup_path = file.path(lake_dir, "backups")
)

# To restore from backup:
# detach_ducklake("my_lake")
# attach_ducklake("my_lake", lake_path = backup_dir)
} # }
```
