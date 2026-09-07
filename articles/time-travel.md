# Time Travel Queries

``` r

library(ducklake)
library(dplyr)
```

## Introduction

DuckLake’s time travel capabilities provide a powerful audit trail for
your data, enabling you to:

- View data as it existed at any specific point in time
- Query specific versions of your tables
- Restore tables to previous states
- Track the complete history of changes
- Meet regulatory and compliance requirements

This functionality is especially valuable in domains where data
provenance and reproducibility are critical, such as clinical trials,
financial reporting, and scientific research.

## Setting Up the Data Lake

We’ll start by creating a new DuckLake and loading the mtcars dataset.
We’ll then make several modifications to demonstrate time travel
functionality.

``` r

# Install the ducklake extension (required once per system)
# The ducklake extension only needs installing once per machine:
# install_ducklake()

# Create or attach to a data lake
attach_ducklake(
  ducklake_name = "time_travel_demo",
  lake_path = vignette_temp_dir
)

# Create initial table with the mtcars dataset
with_transaction(
  create_table(mtcars, "cars"),
  author = "Data Engineer",
  commit_message = "Initial load of mtcars dataset"
)
#> Committed snapshot 1 (Data Engineer): Initial load of mtcars dataset

# Verify the table was created
get_ducklake_table("cars") |>
  select(mpg, cyl, hp, wt) |>
  head()
#> # A query:  ?? x 4
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpeYD6Bm/ducklake/ducklake2e293c9d21b6.duckdb]
#>     mpg   cyl    hp    wt
#>   <dbl> <dbl> <dbl> <dbl>
#> 1  21       6   110  2.62
#> 2  21       6   110  2.88
#> 3  22.8     4    93  2.32
#> 4  21.4     6   110  3.22
#> 5  18.7     8   175  3.44
#> 6  18.1     6   105  3.46
```

## Making Changes Over Time

Let’s make several changes to our data to create a version history we
can explore.

### Version 1: Initial data

We already have our initial dataset. Let’s check the current state:

``` r

get_ducklake_table("cars") |>
  summarise(
    n_cars = n(),
    avg_mpg = mean(mpg, na.rm = TRUE),
    avg_hp = mean(hp, na.rm = TRUE)
  )
#> # A query:  ?? x 3
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpeYD6Bm/ducklake/ducklake2e293c9d21b6.duckdb]
#>   n_cars avg_mpg avg_hp
#>    <dbl>   <dbl>  <dbl>
#> 1     32    20.1   147.
```

### Version 2: Update fuel efficiency data

Suppose we discover that fuel efficiency measurements need to be
adjusted for some vehicles:

``` r

# Update mpg for high-performance cars (5% reduction), in place: the
# filter becomes the WHERE clause of an UPDATE
with_transaction(
  get_ducklake_table("cars") |>
    filter(hp > 200) |>
    mutate(mpg = mpg * 0.95) |>
    ducklake_exec(),
  author = "Data Analyst",
  commit_message = "Adjust MPG for high-performance vehicles"
)
#> Committed snapshot 2 (Data Analyst): Adjust MPG for high-performance
#> vehicles

# Check the updated averages
get_ducklake_table("cars") |>
  summarise(
    n_cars = n(),
    avg_mpg = mean(mpg, na.rm = TRUE),
    avg_hp = mean(hp, na.rm = TRUE)
  )
#> # A query:  ?? x 3
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpeYD6Bm/ducklake/ducklake2e293c9d21b6.duckdb]
#>   n_cars avg_mpg avg_hp
#>    <dbl>   <dbl>  <dbl>
#> 1     32    19.9   147.
```

### Version 3: Add efficiency classification

Let’s add a new categorical variable to classify cars by fuel
efficiency:

``` r

# A new column is a metadata change; filling it is an in-database UPDATE
with_transaction({
  add_table_column("cars", "efficiency_class", "VARCHAR")
  get_ducklake_table("cars") |>
    mutate(
      efficiency_class = case_when(
        mpg >= 25 ~ "High",
        mpg >= 20 ~ "Medium",
        TRUE ~ "Low"
      )
    ) |>
    ducklake_exec()
},
  author = "Data Analyst",
  commit_message = "Add efficiency classification"
)
#> Added column "efficiency_class" (VARCHAR) to "cars".
#> ℹ Metadata-only change; no data files were rewritten.
#> Committed snapshot 3 (Data Analyst): Add efficiency classification

# View the new classification
get_ducklake_table("cars") |>
  count(efficiency_class) |>
  arrange(desc(n))
#> # A query:    ?? x 2
#> # Database:   DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpeYD6Bm/ducklake/ducklake2e293c9d21b6.duckdb]
#> # Ordered by: desc(n)
#>   efficiency_class     n
#>   <chr>            <dbl>
#> 1 Low                 18
#> 2 Medium               8
#> 3 High                 6
```

### Version 4: Correct an error

Suppose we realize the efficiency classification thresholds were wrong
and need to be corrected:

``` r

with_transaction(
  get_ducklake_table("cars") |>
    mutate(
      efficiency_class = case_when(
        mpg >= 30 ~ "High",
        mpg >= 20 ~ "Medium",
        TRUE ~ "Low"
      )
    ) |>
    ducklake_exec(),
  author = "Senior Analyst",
  commit_message = "Correct efficiency classification thresholds"
)
#> Committed snapshot 4 (Senior Analyst): Correct efficiency classification
#> thresholds

# View the corrected classification
get_ducklake_table("cars") |>
  count(efficiency_class) |>
  arrange(desc(n))
#> # A query:    ?? x 2
#> # Database:   DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpeYD6Bm/ducklake/ducklake2e293c9d21b6.duckdb]
#> # Ordered by: desc(n)
#>   efficiency_class     n
#>   <chr>            <dbl>
#> 1 Low                 18
#> 2 Medium              10
#> 3 High                 4
```

## Exploring Version History

Now that we have a history of changes, let’s explore the time travel
functionality.

### List all snapshots

``` r

# View all available versions of the table
snapshots <- list_table_snapshots("cars")
snapshots
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-09-07 05:07:45              1
#> 2           2 2026-09-07 05:07:46              1
#> 3           3 2026-09-07 05:07:46              2
#> 4           4 2026-09-07 05:07:46              2
#>                                                                                 changes
#> 1                                    tables_created, tables_inserted_into, main.cars, 1
#> 2                                                  inlined_insert, inlined_delete, 1, 1
#> 3 tables_altered, tables_inserted_into, tables_deleted_from, inlined_delete, 1, 1, 1, 1
#> 4                                       tables_inserted_into, tables_deleted_from, 1, 1
#>           author                               commit_message commit_extra_info
#> 1  Data Engineer               Initial load of mtcars dataset              <NA>
#> 2   Data Analyst     Adjust MPG for high-performance vehicles              <NA>
#> 3   Data Analyst                Add efficiency classification              <NA>
#> 4 Senior Analyst Correct efficiency classification thresholds              <NA>
```

### Query a specific version

Let’s look at version 2, before we added the efficiency classification:

``` r

# Get version 2 (after MPG adjustment, before classification)
get_ducklake_table_version("cars", version = 2) |>
  select(mpg, cyl, hp, wt) |>
  head()
#> # A query:  ?? x 4
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpeYD6Bm/ducklake/ducklake2e293c9d21b6.duckdb]
#>     mpg   cyl    hp    wt
#>   <dbl> <dbl> <dbl> <dbl>
#> 1  21       6   110  2.62
#> 2  21       6   110  2.88
#> 3  22.8     4    93  2.32
#> 4  21.4     6   110  3.22
#> 5  18.7     8   175  3.44
#> 6  18.1     6   105  3.46

# Notice: no efficiency_class column yet
```

Compare this with version 3, which has the classification:

``` r

# Get version 3 (with initial classification)
get_ducklake_table_version("cars", version = 3) |>
  select(mpg, efficiency_class) |>
  count(efficiency_class)
#> # A query:  ?? x 2
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpeYD6Bm/ducklake/ducklake2e293c9d21b6.duckdb]
#>   efficiency_class     n
#>   <chr>            <dbl>
#> 1 High                 6
#> 2 Medium               8
#> 3 Low                 18
```

### Query data as of a specific timestamp

We can also query data as it existed at any point in time:

``` r

# Get the timestamp of the second snapshot (the MPG adjustment)
version2_timestamp <- snapshots$snapshot_time[[2]]

# Query data as it existed at that time
# Note: Add 1 second to ensure we query AFTER the snapshot was created
get_ducklake_table_asof("cars", version2_timestamp + 1) |>
  summarise(
    avg_mpg = mean(mpg, na.rm = TRUE)
  )
#> # A query:  ?? x 1
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpeYD6Bm/ducklake/ducklake2e293c9d21b6.duckdb]
#>   avg_mpg
#>     <dbl>
#> 1    19.9
```

## Comparing Versions

One powerful use case is comparing different versions to understand what
changed:

``` r

# Get MPG values from version 1 (original) and version 2 (after adjustment)
original <- get_ducklake_table_version("cars", version = 1) |>
  select(mpg) |>
  collect() |>
  mutate(version = "Original")

adjusted <- get_ducklake_table_version("cars", version = 2) |>
  select(mpg) |>
  collect() |>
  mutate(version = "Adjusted")

# Combine and compare
bind_rows(original, adjusted) |>
  group_by(version) |>
  summarise(
    avg_mpg = mean(mpg, na.rm = TRUE),
    min_mpg = min(mpg),
    max_mpg = max(mpg)
  )
#> # A tibble: 2 × 4
#>   version  avg_mpg min_mpg max_mpg
#>   <chr>      <dbl>   <dbl>   <dbl>
#> 1 Adjusted    19.9    9.88    33.9
#> 2 Original    20.1   10.4     33.9
```

## Restoring Previous Versions

If we need to undo changes,
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)
rolls a table back to an earlier snapshot in one call:

``` r

# Go back to version 2 (before adding classifications)
restore_table_version("cars", version = 2, author = "Senior Analyst")
#> Committed snapshot 5 (Senior Analyst): Restored cars to snapshot 2

# Verify the restoration - efficiency_class column should be gone
get_ducklake_table("cars") |> colnames()
#>  [1] "mpg"  "cyl"  "disp" "hp"   "drat" "wt"   "qsec" "vs"   "am"   "gear"
#> [11] "carb"
```

You can also restore to a point in time with
`restore_table_version("cars", timestamp = "2026-07-01 09:00:00")`, and
pass a custom `commit_message` if the default (“Restored cars to
snapshot 2”) isn’t descriptive enough for your audit trail.

Nothing is lost in a restore: the rollback happens *forward*, as a new
snapshot with its own author and commit message, so the full history —
including the states after the restore point — remains available for
time travel. That also means a restore is itself reversible with another
[`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)
call:

``` r

list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-09-07 05:07:45              1
#> 2           2 2026-09-07 05:07:46              1
#> 3           3 2026-09-07 05:07:46              2
#> 4           4 2026-09-07 05:07:46              2
#> 5           5 2026-09-07 05:07:47              3
#>                                                                                 changes
#> 1                                    tables_created, tables_inserted_into, main.cars, 1
#> 2                                                  inlined_insert, inlined_delete, 1, 1
#> 3 tables_altered, tables_inserted_into, tables_deleted_from, inlined_delete, 1, 1, 1, 1
#> 4                                       tables_inserted_into, tables_deleted_from, 1, 1
#> 5                 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#>           author                               commit_message commit_extra_info
#> 1  Data Engineer               Initial load of mtcars dataset              <NA>
#> 2   Data Analyst     Adjust MPG for high-performance vehicles              <NA>
#> 3   Data Analyst                Add efficiency classification              <NA>
#> 4 Senior Analyst Correct efficiency classification thresholds              <NA>
#> 5 Senior Analyst                  Restored cars to snapshot 2              <NA>
```

## Pinning a Whole Session to a Snapshot

The queries above travel one table at a time. To freeze *everything* —
say, to re-run a report exactly as it stood at a submission milestone —
attach the lake pinned to a snapshot:

``` r

attach_ducklake(
  "cars_milestone",
  lake_path = "~/data/lake",
  snapshot_version = 2
)
```

Every table then reads as of snapshot 2 with no `AT (...)` clauses
needed, and writes are rejected, so the milestone view can’t drift. A
`snapshot_time` argument does the same for a point in time.

## Row Lineage

Every row in a DuckLake table carries two hidden columns: `rowid`, an
identifier assigned when the row was first inserted and kept through
updates and compaction, and `snapshot_id`, the snapshot that wrote the
row’s current version. They are how the change feed tells an update
apart from a delete followed by an insert. dbplyr does not know about
hidden columns, so read them with a SQL query:

``` r

conn <- get_ducklake_connection()
tbl(conn, sql("SELECT rowid, snapshot_id, mpg, cyl FROM cars")) |>
  head(5) |>
  collect()
#> # A tibble: 5 × 4
#>   rowid snapshot_id   mpg   cyl
#>   <dbl>       <dbl> <dbl> <dbl>
#> 1     0           5  21       6
#> 2     1           5  21       6
#> 3     2           5  22.8     4
#> 4     3           5  21.4     6
#> 5     4           5  18.7     8
```

## Use Cases for Time Travel

Time travel functionality is particularly valuable for:

1.  **Regulatory Compliance**: Maintain complete audit trails for
    datasets used in regulatory submissions (e.g., clinical trials,
    financial reporting)
2.  **Reproducibility**: Recreate analyses exactly as they were run at
    specific points in time
3.  **Data Recovery**: Restore accidentally modified or deleted data
4.  **Change Tracking**: Understand when and how data quality issues
    were introduced
5.  **Reporting**: Generate historical reports using data as it existed
    at specific time points
6.  **Collaboration**: Allow team members to reference specific versions
    of shared datasets
7.  **Debugging**: Identify when unexpected changes occurred in your
    data pipeline

## Metadata and Audit Information

Each snapshot includes metadata about when it was created and what
changes were made. The
[`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md)
function provides a complete audit trail:

``` r

# Get detailed snapshot history with all metadata
snapshot_history <- list_table_snapshots("cars")
snapshot_history |>
  select(snapshot_id, snapshot_time, author, commit_message)
#>   snapshot_id       snapshot_time         author
#> 1           1 2026-09-07 05:07:45  Data Engineer
#> 2           2 2026-09-07 05:07:46   Data Analyst
#> 3           3 2026-09-07 05:07:46   Data Analyst
#> 4           4 2026-09-07 05:07:46 Senior Analyst
#> 5           5 2026-09-07 05:07:47 Senior Analyst
#>                                 commit_message
#> 1               Initial load of mtcars dataset
#> 2     Adjust MPG for high-performance vehicles
#> 3                Add efficiency classification
#> 4 Correct efficiency classification thresholds
#> 5                  Restored cars to snapshot 2
```

This complete audit trail ensures that you can always answer questions
like:

- What changes were made?
- When were they made?
- What version is the table at?
- What was the data before this change?

You can also access metadata about all tables in the DuckLake:

``` r

# View metadata for all tables
all_snapshots <- list_table_snapshots()
all_snapshots |>
  select(snapshot_id, snapshot_time, changes) |>
  head(10)
#>   snapshot_id       snapshot_time
#> 1           0 2026-09-07 05:07:45
#> 2           1 2026-09-07 05:07:45
#> 3           2 2026-09-07 05:07:46
#> 4           3 2026-09-07 05:07:46
#> 5           4 2026-09-07 05:07:46
#> 6           5 2026-09-07 05:07:47
#>                                                                                 changes
#> 1                                                                 schemas_created, main
#> 2                                    tables_created, tables_inserted_into, main.cars, 1
#> 3                                                  inlined_insert, inlined_delete, 1, 1
#> 4 tables_altered, tables_inserted_into, tables_deleted_from, inlined_delete, 1, 1, 1, 1
#> 5                                       tables_inserted_into, tables_deleted_from, 1, 1
#> 6                 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
```
