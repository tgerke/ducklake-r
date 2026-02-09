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
install_ducklake()

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
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Verify the table was created
get_ducklake_table("cars") |>
  select(mpg, cyl, hp, wt) |>
  head()
#> # Source:   SQL [?? x 4]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpjOgEUE/duckplyr/duckplyr2136299a7926.duckdb]
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
#> # Source:   SQL [?? x 3]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpjOgEUE/duckplyr/duckplyr2136299a7926.duckdb]
#>   n_cars avg_mpg avg_hp
#>    <dbl>   <dbl>  <dbl>
#> 1     32    20.1   147.
```

### Version 2: Update fuel efficiency data

Suppose we discover that fuel efficiency measurements need to be
adjusted for some vehicles:

``` r
# Update mpg for high-performance cars (5% reduction)
with_transaction(
  get_ducklake_table("cars") |>
    mutate(mpg = if_else(hp > 200, mpg * 0.95, mpg)) |>
    replace_table("cars"),
  author = "Data Analyst",
  commit_message = "Adjust MPG for high-performance vehicles"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Check the updated averages
get_ducklake_table("cars") |>
  summarise(
    n_cars = n(),
    avg_mpg = mean(mpg, na.rm = TRUE),
    avg_hp = mean(hp, na.rm = TRUE)
  )
#> # Source:   SQL [?? x 3]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpjOgEUE/duckplyr/duckplyr2136299a7926.duckdb]
#>   n_cars avg_mpg avg_hp
#>    <dbl>   <dbl>  <dbl>
#> 1     32    19.9   147.
```

### Version 3: Add efficiency classification

Let’s add a new categorical variable to classify cars by fuel
efficiency:

``` r
with_transaction(
  get_ducklake_table("cars") |>
    mutate(
      efficiency_class = case_when(
        mpg >= 25 ~ "High",
        mpg >= 20 ~ "Medium",
        TRUE ~ "Low"
      )
    ) |>
    replace_table("cars"),
  author = "Data Analyst",
  commit_message = "Add efficiency classification"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# View the new classification
get_ducklake_table("cars") |>
  count(efficiency_class) |>
  arrange(desc(n))
#> # Source:     SQL [?? x 2]
#> # Database:   DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpjOgEUE/duckplyr/duckplyr2136299a7926.duckdb]
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
    replace_table("cars"),
  author = "Senior Analyst",
  commit_message = "Correct efficiency classification thresholds"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# View the corrected classification
get_ducklake_table("cars") |>
  count(efficiency_class) |>
  arrange(desc(n))
#> # Source:     SQL [?? x 2]
#> # Database:   DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpjOgEUE/duckplyr/duckplyr2136299a7926.duckdb]
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
#> 2           1 2026-02-09 21:11:06              1
#> 3           2 2026-02-09 21:11:07              2
#> 4           3 2026-02-09 21:11:07              3
#> 5           4 2026-02-09 21:11:07              4
#>                                                                 changes
#> 2                    tables_created, tables_inserted_into, main.cars, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 4 tables_created, tables_dropped, tables_inserted_into, main.cars, 2, 3
#> 5 tables_created, tables_dropped, tables_inserted_into, main.cars, 3, 4
#>           author                               commit_message commit_extra_info
#> 2  Data Engineer               Initial load of mtcars dataset              <NA>
#> 3   Data Analyst     Adjust MPG for high-performance vehicles              <NA>
#> 4   Data Analyst                Add efficiency classification              <NA>
#> 5 Senior Analyst Correct efficiency classification thresholds              <NA>
```

### Query a specific version

Let’s look at version 2, before we added the efficiency classification:

``` r
# Get version 2 (after MPG adjustment, before classification)
get_ducklake_table_version("cars", version = 2) |>
  select(mpg, cyl, hp, wt) |>
  head()
#> # Source:   SQL [?? x 4]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpjOgEUE/duckplyr/duckplyr2136299a7926.duckdb]
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
#> # Source:   SQL [?? x 2]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpjOgEUE/duckplyr/duckplyr2136299a7926.duckdb]
#>   efficiency_class     n
#>   <chr>            <dbl>
#> 1 Medium               8
#> 2 Low                 18
#> 3 High                 6
```

### Query data as of a specific timestamp

We can also query data as it existed at any point in time:

``` r
# Get the timestamp from version 2
version2_timestamp <- snapshots |>
  filter(schema_version == 2) |>
  pull(snapshot_time)

# Query data as it existed at that time
# Note: Add 1 second to ensure we query AFTER the snapshot was created
get_ducklake_table_asof("cars", version2_timestamp + 1) |>
  summarise(
    avg_mpg = mean(mpg, na.rm = TRUE)
  )
#> # Source:   SQL [?? x 1]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmpjOgEUE/duckplyr/duckplyr2136299a7926.duckdb]
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

If we need to undo changes, we can restore a table to a previous version
by reading that version and replacing the current table:

``` r
# Let's say we want to go back to version 2 (before adding classifications)
# We restore by reading version 2 and replacing the current table
with_transaction(
  get_ducklake_table_version("cars", version = 2) |>
    replace_table("cars"),
  author = "Senior Analyst", 
  commit_message = "Restore to version 2 (before efficiency classification)"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Verify the restoration - efficiency_class column should be gone
get_ducklake_table("cars") |> colnames()
#>  [1] "mpg"  "cyl"  "disp" "hp"   "drat" "wt"   "qsec" "vs"   "am"   "gear"
#> [11] "carb"
```

After restoring, we can see that the efficiency_class column is no
longer present. A new snapshot is created for the restoration:

``` r
list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 2           1 2026-02-09 21:11:06              1
#> 3           2 2026-02-09 21:11:07              2
#> 4           3 2026-02-09 21:11:07              3
#> 5           4 2026-02-09 21:11:07              4
#> 6           5 2026-02-09 21:11:08              5
#>                                                                 changes
#> 2                    tables_created, tables_inserted_into, main.cars, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 4 tables_created, tables_dropped, tables_inserted_into, main.cars, 2, 3
#> 5 tables_created, tables_dropped, tables_inserted_into, main.cars, 3, 4
#> 6 tables_created, tables_dropped, tables_inserted_into, main.cars, 4, 5
#>           author                                          commit_message
#> 2  Data Engineer                          Initial load of mtcars dataset
#> 3   Data Analyst                Adjust MPG for high-performance vehicles
#> 4   Data Analyst                           Add efficiency classification
#> 5 Senior Analyst            Correct efficiency classification thresholds
#> 6 Senior Analyst Restore to version 2 (before efficiency classification)
#>   commit_extra_info
#> 2              <NA>
#> 3              <NA>
#> 4              <NA>
#> 5              <NA>
#> 6              <NA>
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
#> 2           1 2026-02-09 21:11:06  Data Engineer
#> 3           2 2026-02-09 21:11:07   Data Analyst
#> 4           3 2026-02-09 21:11:07   Data Analyst
#> 5           4 2026-02-09 21:11:07 Senior Analyst
#> 6           5 2026-02-09 21:11:08 Senior Analyst
#>                                            commit_message
#> 2                          Initial load of mtcars dataset
#> 3                Adjust MPG for high-performance vehicles
#> 4                           Add efficiency classification
#> 5            Correct efficiency classification thresholds
#> 6 Restore to version 2 (before efficiency classification)
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
#> 1           0 2026-02-09 21:11:06
#> 2           1 2026-02-09 21:11:06
#> 3           2 2026-02-09 21:11:07
#> 4           3 2026-02-09 21:11:07
#> 5           4 2026-02-09 21:11:07
#> 6           5 2026-02-09 21:11:08
#>                                                                 changes
#> 1                                                 schemas_created, main
#> 2                    tables_created, tables_inserted_into, main.cars, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 4 tables_created, tables_dropped, tables_inserted_into, main.cars, 2, 3
#> 5 tables_created, tables_dropped, tables_inserted_into, main.cars, 3, 4
#> 6 tables_created, tables_dropped, tables_inserted_into, main.cars, 4, 5
```
