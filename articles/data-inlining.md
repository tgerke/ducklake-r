# Data Inlining

Data inlining is one of the flagship features of DuckLake v1.0. When
writing small changesets, DuckLake stores the data directly in the
catalog database instead of creating individual Parquet files. This
eliminates the “small files problem” that plagues traditional data lake
formats during streaming and frequent-update workloads.

Inlining is **enabled by default** with a row threshold of 10. No
configuration is required to benefit from it.

``` r

library(ducklake)
library(dplyr)
```

## How it works

Every insert, delete, or update that affects fewer rows than the
inlining threshold is stored in a lightweight table inside the catalog
(DuckDB, SQLite or PostgreSQL) instead of writing a Parquet file to the
data path. At query time DuckLake seamlessly combines inlined and
Parquet data, so results are always correct regardless of where data
lives.

When you’re ready, you can **flush** the inlined data to consolidated
Parquet files with a single call.

## Setup

``` r

attach_ducklake("sensor_lake", lake_path = vignette_temp_dir)
```

## Small writes are automatically inlined

Start with a small sensor-readings table. The data has only 3 rows, well
below the default inlining threshold of 10, so DuckLake stores it
directly in the catalog instead of writing a Parquet file.

``` r

readings <- data.frame(
  sensor_id = 1:3,
  temperature = c(21.5, 22.1, 21.8),
  ts = as.POSIXct(
    c("2025-03-27 10:00:00", "2025-03-27 10:00:10", "2025-03-27 10:00:20"),
    tz = "UTC"
  )
)

with_transaction(
  create_table(readings, "readings"),
  author = "Sensor Team",
  commit_message = "Initial sensor readings"
)
#> Committed snapshot 1 (Sensor Team): Initial sensor readings
```

No Parquet files were created. All of the data is inlined in the
catalog:

``` r

conn <- get_ducklake_connection()
DBI::dbGetQuery(
  conn,
  sprintf("SELECT count(*) AS parquet_files FROM glob('%s/**/*.parquet');", vignette_temp_dir)
)
#>   parquet_files
#> 1             0
```

Yet queries return all rows seamlessly:

``` r

get_ducklake_table("readings") |> collect()
#> # A tibble: 3 × 3
#>   sensor_id temperature ts                 
#>       <int>       <dbl> <dttm>             
#> 1         1        21.5 2025-03-27 10:00:00
#> 2         2        22.1 2025-03-27 10:00:10
#> 3         3        21.8 2025-03-27 10:00:20
```

## Incremental updates stay inlined

Small modifications also stay in the catalog when the changeset is below
the threshold. Adding a derived column is a metadata change
([`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)),
and filling it is an in-database UPDATE:

``` r

with_transaction({
  add_table_column("readings", "temp_calibrated", "DOUBLE")
  get_ducklake_table("readings") |>
    mutate(temp_calibrated = temperature - 0.3) |>
    ducklake_exec()
},
  author = "Sensor Team",
  commit_message = "Add calibrated temperature"
)
#> Added column "temp_calibrated" (DOUBLE) to "readings".
#> ℹ Metadata-only change; no data files were rewritten.
#> Committed snapshot 2 (Sensor Team): Add calibrated temperature

get_ducklake_table("readings") |> collect()
#> # A tibble: 3 × 4
#>   sensor_id temperature ts                  temp_calibrated
#>       <int>       <dbl> <dttm>                        <dbl>
#> 1         1        21.5 2025-03-27 10:00:00            21.2
#> 2         2        22.1 2025-03-27 10:00:10            21.8
#> 3         3        21.8 2025-03-27 10:00:20            21.5
```

## Removing rows

A small delete is inlined too: DuckLake records it in the catalog rather
than writing a delete file.

``` r

# Remove sensor 2's reading
with_transaction(
  rows_delete(
    get_ducklake_table("readings"),
    data.frame(sensor_id = 2L),
    by = "sensor_id"
  ),
  author = "Sensor Team",
  commit_message = "Remove faulty sensor 2 reading"
)
#> Committed snapshot 3 (Sensor Team): Remove faulty sensor 2 reading

get_ducklake_table("readings") |> collect()
#> # A tibble: 2 × 4
#>   sensor_id temperature ts                  temp_calibrated
#>       <int>       <dbl> <dttm>                        <dbl>
#> 1         1        21.5 2025-03-27 10:00:00            21.2
#> 2         3        21.8 2025-03-27 10:00:20            21.5
```

## Large writes bypass inlining automatically

When a write exceeds the threshold, DuckLake writes directly to Parquet,
with no configuration needed:

``` r

# 50 rows, well above the default threshold of 10
large_batch <- data.frame(
  sensor_id = 101:150,
  temperature = rnorm(50, mean = 22, sd = 1),
  ts = seq(as.POSIXct("2025-03-28 00:00:00", tz = "UTC"), by = "10 sec", length.out = 50),
  temp_calibrated = rnorm(50, mean = 21.7, sd = 1)
)

with_transaction(
  create_table(large_batch, "readings_bulk"),
  author = "Sensor Team",
  commit_message = "Bulk sensor upload"
)
#> Committed snapshot 4 (Sensor Team): Bulk sensor upload

# This table has a Parquet file
DBI::dbGetQuery(
  conn,
  sprintf("SELECT count(*) AS parquet_files FROM glob('%s/**/*.parquet');", vignette_temp_dir)
)
#>   parquet_files
#> 1             1
```

## Configuring the inlining threshold

### Global default

Change the threshold for all tables in the session:

``` r

# Increase the threshold for a streaming workload
set_inlining_row_limit(50)
#> Global data inlining row limit set to 50.
get_inlining_row_limit()
#> [1] 50
```

### Per-connection at attach time

Set the threshold when attaching a DuckLake (not persisted):

``` r

attach_ducklake(
  "streaming_lake",
  lake_path = "/data/streaming",
  data_inlining_row_limit = 100
)
```

### Per-table persistent override

Set a table-level limit that is stored in the catalog and survives
reconnects:

``` r

set_inlining_row_limit(100, table_name = "readings")
```

### Disable inlining

``` r

set_inlining_row_limit(0)
#> Global data inlining row limit set to 0.
# All writes now go directly to Parquet, even single rows
```

## Flushing inlined data to Parquet

When inlined data accumulates, flush it to consolidated Parquet files:

``` r

flush_result <- flush_inlined_data()
#> Flushed 6 rows from 1 table to Parquet.
flush_result
#>   schema_name table_name rows_flushed
#> 1        main   readings            6
```

Data remains correct after flushing:

``` r

get_ducklake_table("readings") |> collect()
#> # A tibble: 2 × 4
#>   sensor_id temperature ts                  temp_calibrated
#>       <int>       <dbl> <dttm>                        <dbl>
#> 1         1        21.5 2025-03-27 10:00:00            21.2
#> 2         3        21.8 2025-03-27 10:00:20            21.5
```

### Flushing a specific table

``` r

flush_inlined_data(table_name = "readings")
```

## Checkpoint: one-stop maintenance

[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)
runs DuckLake’s maintenance operations in sequence: it flushes inlined
data, merges small files, and rewrites heavily deleted files. It also
expires old snapshots and deletes the files they released, but only once
the lake carries a retention policy (the `expire_older_than` and
`delete_older_than` options); see
[`vignette("storage-and-backups")`](https://tgerke.github.io/ducklake-r/articles/storage-and-backups.md).

``` r

checkpoint_ducklake()
#> Checkpoint completed for "sensor_lake".
```

Run checkpoints periodically (e.g., after a batch of streaming inserts)
to keep query performance optimal and consolidate inlined data.

Note for Windows users: with a DuckDB-file catalog, the file-cleanup
step of `CHECKPOINT` can fail because Windows does not allow the catalog
file to be opened a second time while the lake is attached (a current
DuckDB limitation).
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md)
is unaffected; on Windows, prefer it for routine use and run full
checkpoints from a fresh session, or use a PostgreSQL/SQLite catalog.

## Time travel with inlined data

Inlined data fully supports DuckLake’s time-travel capabilities. Each
inlined insert or delete creates a snapshot, just like a regular write:

``` r

# Check available snapshots
snapshots <- list_table_snapshots("readings")
snapshots
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-09-09 17:12:35              1
#> 2           2 2026-09-09 17:12:36              2
#> 3           3 2026-09-09 17:12:36              2
#> 4           5 2026-09-09 17:12:37              3
#> 5           6 2026-09-09 17:12:37              3
#>                                                   changes      author
#> 1        tables_created, inlined_insert, main.readings, 1 Sensor Team
#> 2 tables_altered, inlined_insert, inlined_delete, 1, 1, 1 Sensor Team
#> 3                                       inlined_delete, 1 Sensor Team
#> 4                                      flushed_inlined, 1        <NA>
#> 5                                       rewrite_delete, 1        <NA>
#>                   commit_message commit_extra_info
#> 1        Initial sensor readings              <NA>
#> 2     Add calibrated temperature              <NA>
#> 3 Remove faulty sensor 2 reading              <NA>
#> 4                           <NA>              <NA>
#> 5                           <NA>              <NA>
```

``` r

# Query an earlier version
if (nrow(snapshots) > 0) {
  first_version <- snapshots$snapshot_id[1]
  get_ducklake_table_version("readings", first_version) |> collect()
}
#> # A tibble: 3 × 3
#>   sensor_id temperature ts                 
#>       <int>       <dbl> <dttm>             
#> 1         1        21.5 2025-03-27 10:00:00
#> 2         2        22.1 2025-03-27 10:00:10
#> 3         3        21.8 2025-03-27 10:00:20
```

## When to use inlining

| Workload                   | Recommendation                       |
|----------------------------|--------------------------------------|
| Streaming / IoT sensors    | Increase threshold (e.g., 50 to 100) |
| Periodic large batch loads | Default (10) is fine                 |
| Single-row corrections     | Default handles it automatically     |
| Append-only bulk ETL       | Consider disabling (`limit = 0`)     |

## What inlining does *not* change

Data inlining is a **storage optimisation**, not a change to DuckLake’s
data integrity model. Whether data is inlined or written to Parquet: -
ACID transactions are fully enforced. - Every modification creates a
snapshot with author, timestamp, and commit message metadata. -
Time-travel queries work identically. - The audit trail is complete and
unaffected.

The only difference is *where* the data physically resides until it is
flushed: in the catalog database (inlined) or in Parquet files on the
data path.

### Regulated environments

In settings governed by ICH E6(R2), 21 CFR Part 11, or similar
guidelines, inlining is fully compatible with compliance requirements.
Clinical trial workflows typically involve batch loads of full SDTM or
ADaM domains, which exceed the default threshold and go directly to
Parquet. Inlining primarily benefits small targeted corrections (e.g.,
updating a single subject’s flag), keeping those lightweight without
generating unnecessary files.

Before archival or regulatory submission, run
[`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)
(or
[`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md))
to materialise all inlined data to Parquet. This ensures that the data
path contains a fully self-describing set of Parquet files suitable for
long-term storage and portability. See the [Clinical Trial Data
Lake](https://tgerke.github.io/ducklake-r/articles/clinical-trial-datalake.html)
article for a full regulated-workflow example.

## Summary

- **Inlining is on by default** (threshold = 10 rows). No setup
  required.
- Small inserts, deletes, and updates are stored in the catalog.
- Use
  [`flush_inlined_data()`](https://tgerke.github.io/ducklake-r/reference/flush_inlined_data.md)
  or
  [`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)
  when ready to materialise to Parquet.
- Full time-travel support is preserved for inlined data.
- Adjust the threshold with
  [`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)
  or at attach time via `data_inlining_row_limit`.

For more details, see the [DuckLake data inlining
documentation](https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining.html).
