# Visualizing Your Lake

``` r

library(ducklake)
library(dplyr)
```

## Introduction

A DuckLake keeps detailed records about itself: every change to a table
is a snapshot, every changed row is in the change feed, and every
Parquet file is tracked in the catalog. All of that comes back to R as
data frames, but past a handful of rows a picture is easier to scan.
This vignette walks through the package’s three plotting functions:

- [`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md)
  draws a table’s history as a commit-log style timeline
- [`plot_table_changes()`](https://tgerke.github.io/ducklake-r/reference/plot_table_changes.md)
  shows how many rows each snapshot changed
- [`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md)
  shows how each table’s data is laid out on disk

These require the ggplot2 package, which is suggested (not required) by
ducklake.

## Building Some History

We’ll create a small lake and put a table through a few changes,
recording authors and commit messages along the way.

``` r

install_ducklake()
#> Installed ducklake extension.

attach_ducklake(
  ducklake_name = "lake_viz_demo",
  lake_path = vignette_temp_dir
)

fleet <- tibble(
  car_id = 1:5,
  model = c("Corolla", "Civic", "Model 3", "F-150", "Outback"),
  mileage = c(42000, 38000, 12000, 67000, 55000)
)

with_transaction(
  create_table(fleet, "fleet"),
  author = "Data Engineer",
  commit_message = "Initial fleet load"
)
#> Transaction started.
#> Transaction committed.
```

Now a couple of routine updates:

``` r

new_cars <- tibble(
  car_id = 6:7,
  model = c("CX-5", "Ioniq 5"),
  mileage = c(21000, 8000)
)

with_transaction(
  rows_insert(get_ducklake_table("fleet"), new_cars, by = "car_id"),
  author = "Fleet Manager",
  commit_message = "Add March arrivals"
)
#> Transaction started.
#> Transaction committed.

serviced <- tibble(
  car_id = c(1, 3, 5),
  mileage = c(42750, 13400, 55900)
)

with_transaction(
  rows_update(get_ducklake_table("fleet"), serviced, by = "car_id"),
  author = "Fleet Manager",
  commit_message = "Record spring odometer readings"
)
#> Transaction started.
#> Transaction committed.

sold <- tibble(car_id = 4L)

with_transaction(
  rows_delete(get_ducklake_table("fleet"), sold, by = "car_id"),
  author = "Fleet Manager",
  commit_message = "Remove sold F-150"
)
#> Transaction started.
#> Transaction committed.
```

The audit trail so far:

``` r

list_table_snapshots("fleet") |>
  select(snapshot_id, snapshot_time, author, commit_message)
#>   snapshot_id       snapshot_time        author                  commit_message
#> 1           1 2026-07-20 19:43:26 Data Engineer              Initial fleet load
#> 2           2 2026-07-20 19:43:26 Fleet Manager              Add March arrivals
#> 3           3 2026-07-20 19:43:26 Fleet Manager Record spring odometer readings
#> 4           4 2026-07-20 19:43:26 Fleet Manager               Remove sold F-150
```

## Plotting the Timeline

[`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md)
turns that data frame into a picture:

``` r

plot_snapshots("fleet")
```

![](visualizing-your-lake_files/figure-html/plot-table-1.png)

Reading it like a git log: the newest snapshot is at the top, each row
shows the snapshot’s timestamp, author, and commit message, and the
color shows whether the snapshot created the table, changed data,
changed the schema, or was maintenance activity (like flushing inlined
data or compacting files). Snapshots are spaced evenly by order rather
than by clock time, so a table that sat untouched for months stays
readable: a long idle stretch shows up as an inline marker (like “103
days later”) instead of stretching the whole plot.

## How Much Changed

The timeline shows when changes happened and what kind they were;
[`plot_table_changes()`](https://tgerke.github.io/ducklake-r/reference/plot_table_changes.md)
shows how big each one was. It counts the rows each snapshot touched
using DuckLake’s change feed (the same data
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md)
returns) and draws them as diverging bars: rows inserted or updated
above the axis, rows deleted below it.

``` r

plot_table_changes("fleet")
```

![](visualizing-your-lake_files/figure-html/plot-changes-1.png)

## How the Data Is Stored

Behind the scenes, table data lives in Parquet files (very small writes
may be inlined into the catalog until they are flushed out). To make the
storage view interesting, let’s add a larger table alongside `fleet` and
grow it in two batches, so it ends up with two data files:

``` r

set.seed(42)

telemetry <- tibble(
  reading_id = 1:5000,
  car_id = sample(1:7, 5000, replace = TRUE),
  speed = round(runif(5000, 0, 80)),
  fuel = round(runif(5000, 0, 100))
)

create_table(telemetry, "telemetry")

more_readings <- tibble(
  reading_id = 5001:10000,
  car_id = sample(1:7, 5000, replace = TRUE),
  speed = round(runif(5000, 0, 80)),
  fuel = round(runif(5000, 0, 100))
)

rows_insert(get_ducklake_table("telemetry"), more_readings, by = "reading_id")
```

The fleet table’s handful of rows were small enough to be inlined, so we
flush them into a Parquet file to make them visible to the storage
statistics:

``` r

flush_inlined_data()
#> Flushed 10 rows from 1 table to Parquet.
#>   schema_name table_name rows_flushed
#> 1        main      fleet           10
```

[`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md)
reports each table’s file count and size:

``` r

get_table_info()
#>   table_name schema_id table_id                           table_uuid file_count
#> 1      fleet         0        1 019f810d-ff91-7e9d-8bac-b6062aa1cab6          1
#> 2  telemetry         0        2 019f810e-05dd-7f45-b0c0-13abd0e5872c          2
#>   file_size_bytes delete_file_count delete_file_size_bytes
#> 1            1027                 1                   1120
#> 2           65518                 0                      0
```

And
[`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md)
draws it, with delete files (rows removed but not yet compacted away)
stacked in a second color:

``` r

plot_table_files()
```

![](visualizing-your-lake_files/figure-html/plot-files-1.png)

This is the picture to watch as a lake ages: many small files or a
growing delete-file share mean it is time for the compaction tools
covered in
[`vignette("storage-and-backups")`](https://tgerke.github.io/ducklake-r/articles/storage-and-backups.md),
like
[`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md)
and
[`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md).

## Plotting the Whole Lake

With two tables in the lake, we can step back and look at everything at
once. Calling
[`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md)
without a table name switches to a swimlane view: one row per table, one
point per snapshot, with recently active tables at the top. It answers a
different question than the single-table timeline – not “what happened
to this table” but “which tables are active, and what kind of churn does
each one see”:

``` r

plot_snapshots()
```

![](visualizing-your-lake_files/figure-html/plot-lake-1.png)

Snapshots that don’t belong to any table, like the initial schema
creation, appear in a `(lake)` lane. As in the timeline, points are
spaced by order rather than clock time; the x axis labels show each
snapshot’s date.

## Customizing the Plots

Each function returns a regular ggplot object, so you can restyle it
like any other plot. (The single-table timeline draws on a blank canvas,
so it is best customized with `labs()` and `theme()` tweaks rather than
a full theme swap like `theme_classic()`, which would bring back axes
that have no meaning there.)

``` r

plot_snapshots("fleet") +
  ggplot2::labs(title = "Fleet table audit trail")
```

![](visualizing-your-lake_files/figure-html/plot-custom-1.png)

## Learn More

- [`vignette("time-travel")`](https://tgerke.github.io/ducklake-r/articles/time-travel.md)
  covers querying and restoring the historical versions these snapshots
  represent
- [`vignette("transactions")`](https://tgerke.github.io/ducklake-r/articles/transactions.md)
  covers recording authors and commit messages with
  [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
  and
  [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
- [`vignette("storage-and-backups")`](https://tgerke.github.io/ducklake-r/articles/storage-and-backups.md)
  covers the maintenance functions that keep the file layout healthy
