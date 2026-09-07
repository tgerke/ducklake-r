# Create a DuckLake backup

Creates a timestamped backup of the Parquet data files and, for
file-based backends (DuckDB, SQLite), the catalog database. For
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

The catalog is copied with DuckDB's `COPY FROM DATABASE` while the lake
stays attached. The copy is taken inside one transaction, so it is a
consistent snapshot of the metadata, and nothing is detached or shut
down along the way: other attached lakes, in-memory secrets, and a
connection you registered with
[`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)
are left as they are. The data directories (one per schema) are copied
as files.

To work with the backup, attach it with `lake_path` pointing at the
backup directory. Pass `override_data_path = TRUE`, since the copied
catalog remembers the original data location, and `create = FALSE`, so a
mistyped path is an error rather than a new, empty lake. When the
catalog file was not named after the lake (a split layout), name it with
`catalog_connection_string`.

**Important notes:**

- Transactions committed after a backup won't be tracked when
  recovering. The data will exist in the Parquet files, but the backup
  will point to an earlier snapshot.

- Run compaction and cleanup before a backup, not after: they rewrite
  and remove data files that the copied catalog refers to.

- For production systems, schedule backups using `{cronR}` or
  `{taskscheduleR}`.

## See also

Other maintenance:
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md),
[`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
[`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
[`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md),
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md),
[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md),
[`list_ducklake_files()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_files.md),
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md),
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)

## Examples

``` r
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
#> Committed snapshot 1 (User): Initial data

# Create a backup; the lake stays attached throughout
backup_dir <- backup_ducklake(
  ducklake_name = "my_lake",
  lake_path = lake_dir,
  backup_path = file.path(lake_dir, "backups")
)
#> Catalog backed up successfully.
#> Data files backed up successfully (1 directory).
#> Backup completed:
#> /tmp/Rtmpr0HcoA/my_lake1acd373eb9a5/backups/backup_20260907_050617

# Restore (override_data_path needed when location differs):
# detach_ducklake("my_lake")
# attach_ducklake("my_lake", lake_path = backup_dir,
#                 override_data_path = TRUE, create = FALSE)

detach_ducklake("my_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
