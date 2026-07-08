# Create a DuckLake backup

Creates a timestamped backup of the Parquet data files and, for
file-based backends (DuckDB, SQLite), the catalog database file. For
PostgreSQL/MySQL backends only data files are copied; use `pg_dump` /
`mysqldump` for the catalog.

## Usage

``` r
backup_ducklake(ducklake_name, lake_path, backup_path)
```

## Arguments

- ducklake_name:

  Name of the attached DuckLake

- lake_path:

  Path to the DuckLake directory containing the data files (and catalog
  file for DuckDB/SQLite backends)

- backup_path:

  Directory where backups should be stored. A timestamped subdirectory
  will be created within this path.

## Value

Invisibly returns the path to the created backup directory

## Details

For file-based backends the DuckLake is temporarily detached during
backup to release file locks and ensure a consistent copy. It is
automatically re-attached afterwards.

**Important notes:**

- Transactions committed after a backup won't be tracked when
  recovering. The data will exist in the Parquet files, but the backup
  will point to an earlier snapshot.

- Consider coordinating backups with maintenance operations (compaction
  and cleanup) for optimal storage efficiency.

- For production systems, schedule backups using `{cronR}` or
  `{taskscheduleR}`.

## See also

Other maintenance:
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

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

# Restore (override_data_path needed when location differs):
# detach_ducklake("my_lake")
# attach_ducklake("my_lake", lake_path = backup_dir, override_data_path = TRUE)
} # }
```
