# Modifying Tables with Version Control

This vignette demonstrates how to modify tables in a DuckLake while
maintaining complete version control and audit trails. This is essential
for reproducible workflows.

``` r

library(ducklake)
library(dplyr)

# Setup for examples
install_ducklake()
attach_ducklake("modifying_tables_lake", lake_path = vignette_temp_dir)

# Load a sample dataset
with_transaction(
  create_table(mtcars, "cars"),
  author = "Data Engineer",
  commit_message = "Initial car data load"
)
```

A note before we start: when you load dplyr you may see a message that
it masks ducklake’s
[`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
[`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
and
[`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md).
That is harmless. Tables returned by
[`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md)
carry a class that dispatches to ducklake’s DuckLake-aware methods
regardless of the order in which the packages were loaded.

## Choosing a Modification Approach

Good news first: **every committed change to a DuckLake table creates a
snapshot**. Whether you use
[`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
or raw SQL, DuckLake records what changed and you can time-travel back
to any earlier state. (Earlier versions of this vignette said the
`rows_*` functions skip versioning – that is not true in DuckLake v1.0.)
The choice between the two styles is about *what kind of change* you are
making, not about whether it is audited.

### For incremental changes: the `rows_*` functions

Use
[`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
[`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
and
[`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md)
when you are appending records, correcting specific values, or removing
specific rows:

``` r

# Each of these is one SQL statement and one new snapshot
rows_insert(get_ducklake_table("my_table"), new_data, by = "id")
rows_update(get_ducklake_table("my_table"), corrections, by = "id")
rows_delete(get_ducklake_table("my_table"), obsolete_ids, by = "id")
```

**Why they shine for incremental work:**

- **Efficient** - the change runs inside DuckDB as a single statement;
  the rest of the table is never read into R or rewritten
- **Streaming-friendly** - small changes benefit from DuckLake’s [data
  inlining](https://tgerke.github.io/ducklake-r/articles/data-inlining.md),
  landing in the catalog instead of spawning tiny Parquet files
- **Still versioned** - each call produces a snapshot you can
  time-travel to

### For structural or bulk changes: `replace_table()`

Use
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
when the *shape* of the table changes – adding or removing columns – or
when a transformation touches most rows anyway:

``` r

with_transaction(
  get_ducklake_table("my_table") |>
    filter(status == "active") |>
    mutate(processed = TRUE) |>
    replace_table("my_table"),
  author = "Your Name",
  commit_message = "Mark active records as processed"
)
```

[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
collects the transformed data into R and rewrites the table, which is
exactly right for schema changes but wasteful for touching three rows in
a million-row table.

### Group related changes with `with_transaction()`

Whichever style you use, wrap *related* modifications in
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md).
All changes inside the transaction become **one** snapshot, and you can
attach an author and commit message for the audit trail – valuable in
any setting and essential for GxP/21 CFR Part 11 work:

``` r

with_transaction({
  rows_insert(get_ducklake_table("my_table"), march_batch, by = "id")
  rows_delete(get_ducklake_table("my_table"), recalled_units, by = "id")
},
  author = "Data Team",
  commit_message = "March intake; remove recalled units"
)
```

## Examples

### Incremental changes with the `rows_*` functions

Let’s see the row-level functions in action on a small fleet table with
a proper key column:

``` r

fleet <- data.frame(
  car_id = 1:3,
  model = c("Corolla", "Civic", "Model 3"),
  mileage = c(42000, 38500, 12000)
)

with_transaction(
  create_table(fleet, "fleet"),
  author = "Fleet Manager",
  commit_message = "Initial fleet inventory"
)
#> Transaction started.
#> Transaction committed.
```

**Insert** new records by key. The new rows are appended in a single SQL
statement – the existing rows are never read into R:

``` r

new_cars <- data.frame(
  car_id = 4:5,
  model = c("Leaf", "Ioniq 5"),
  mileage = c(500, 120)
)

rows_insert(get_ducklake_table("fleet"), new_cars, by = "car_id")

get_ducklake_table("fleet") |> collect()
#> # A tibble: 5 × 3
#>   car_id model   mileage
#>    <int> <chr>     <dbl>
#> 1      1 Corolla   42000
#> 2      2 Civic     38500
#> 3      3 Model 3   12000
#> 4      4 Leaf        500
#> 5      5 Ioniq 5     120
```

**Update** specific values by key. Only the matched rows change:

``` r

correction <- data.frame(car_id = 2, mileage = 39000)

rows_update(get_ducklake_table("fleet"), correction, by = "car_id")

get_ducklake_table("fleet") |> filter(car_id == 2) |> collect()
#> # A tibble: 1 × 3
#>   car_id model mileage
#>    <int> <chr>   <dbl>
#> 1      2 Civic   39000
```

**Delete** rows by key:

``` r

sold <- data.frame(car_id = 1)

rows_delete(get_ducklake_table("fleet"), sold, by = "car_id")

get_ducklake_table("fleet") |> collect()
#> # A tibble: 4 × 3
#>   car_id model   mileage
#>    <int> <chr>     <dbl>
#> 1      2 Civic     39000
#> 2      3 Model 3   12000
#> 3      4 Leaf        500
#> 4      5 Ioniq 5     120
```

Each call above created its own snapshot. To record an author and commit
message – or to make several row operations land as **one** snapshot –
wrap them in
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md):

``` r

april_arrivals <- data.frame(car_id = 6, model = "ID.4", mileage = 60)
recalled <- data.frame(car_id = 4)

with_transaction({
  rows_insert(get_ducklake_table("fleet"), april_arrivals, by = "car_id")
  rows_delete(get_ducklake_table("fleet"), recalled, by = "car_id")
},
  author = "Fleet Manager",
  commit_message = "April intake; remove recalled Leaf"
)
#> Transaction started.
#> Transaction committed.

# The full history: every change is versioned, wrapped or not
list_table_snapshots("fleet")
#>   snapshot_id       snapshot_time schema_version
#> 1           2 2026-07-08 03:14:49              2
#> 2           3 2026-07-08 03:14:49              2
#> 3           4 2026-07-08 03:14:49              2
#> 4           5 2026-07-08 03:14:50              2
#> 5           6 2026-07-08 03:14:50              2
#>                                         changes        author
#> 1 tables_created, inlined_insert, main.fleet, 2 Fleet Manager
#> 2                             inlined_insert, 2          <NA>
#> 3          inlined_insert, inlined_delete, 2, 2          <NA>
#> 4                             inlined_delete, 2          <NA>
#> 5          inlined_insert, inlined_delete, 2, 2 Fleet Manager
#>                       commit_message commit_extra_info
#> 1            Initial fleet inventory              <NA>
#> 2                               <NA>              <NA>
#> 3                               <NA>              <NA>
#> 4                               <NA>              <NA>
#> 5 April intake; remove recalled Leaf              <NA>
```

### Updating specific rows with `replace_table()`

``` r

# Update mpg values for specific cars (4-cylinder cars get a 5% efficiency boost)
with_transaction(
  get_ducklake_table("cars") |>
    mutate(
      mpg = if_else(cyl == 4, mpg * 1.05, mpg)
    ) |>
    replace_table("cars"),
  author = "Data Engineer",
  commit_message = "Update MPG for 4-cylinder vehicles"
)
#> Transaction started.
#> Transaction committed.

# Check version history - should show the new snapshot
list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-07-08 03:14:49              1
#> 2           7 2026-07-08 03:14:50              3
#>                                                                 changes
#> 1                    tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 3
#>          author                     commit_message commit_extra_info
#> 1 Data Engineer              Initial car data load              <NA>
#> 2 Data Engineer Update MPG for 4-cylinder vehicles              <NA>
```

### Adding derived columns

``` r

# Add new derived columns to existing table
with_transaction(
  get_ducklake_table("cars") |>
    mutate(
      hp_per_cyl = hp / cyl,
      # Add a new flag column
      high_performance = if_else(hp > 200, "Y", "N")
    ) |>
    replace_table("cars"),
  author = "Data Engineer",
  commit_message = "Add HP per cylinder and performance flag"
)
#> Transaction started.
#> Transaction committed.

# Verify new columns exist
get_ducklake_table("cars") |>
  filter(hp > 200) |>
  select(hp, cyl, hp_per_cyl, high_performance)
#> # A query:  ?? x 4
#> # Database: DuckDB 1.5.4 [unknown@Linux 6.17.0-1018-azure:R 4.6.1//tmp/RtmpofHJw6/ducklake/ducklake1f9251e52cb.duckdb]
#>      hp   cyl hp_per_cyl high_performance
#>   <dbl> <dbl>      <dbl> <chr>           
#> 1   245     8       30.6 Y               
#> 2   205     8       25.6 Y               
#> 3   215     8       26.9 Y               
#> 4   230     8       28.8 Y               
#> 5   245     8       30.6 Y               
#> 6   264     8       33   Y               
#> 7   335     8       41.9 Y
```

### Filtering rows with `replace_table()`

``` r

# Keep only specific rows - creates a versioned snapshot
with_transaction(
  get_ducklake_table("cars") |>
    filter(cyl == 8) |>
    replace_table("cars"),
  author = "Data Engineer",
  commit_message = "Filter to V8 engines only"
)
#> Transaction started.
#> Transaction committed.

# Show the filtered table
get_ducklake_table("cars")
#> # A query:  ?? x 13
#> # Database: DuckDB 1.5.4 [unknown@Linux 6.17.0-1018-azure:R 4.6.1//tmp/RtmpofHJw6/ducklake/ducklake1f9251e52cb.duckdb]
#>      mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb hp_per_cyl
#>    <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>      <dbl>
#>  1  18.7     8  360    175  3.15  3.44  17.0     0     0     3     2       21.9
#>  2  14.3     8  360    245  3.21  3.57  15.8     0     0     3     4       30.6
#>  3  16.4     8  276.   180  3.07  4.07  17.4     0     0     3     3       22.5
#>  4  17.3     8  276.   180  3.07  3.73  17.6     0     0     3     3       22.5
#>  5  15.2     8  276.   180  3.07  3.78  18       0     0     3     3       22.5
#>  6  10.4     8  472    205  2.93  5.25  18.0     0     0     3     4       25.6
#>  7  10.4     8  460    215  3     5.42  17.8     0     0     3     4       26.9
#>  8  14.7     8  440    230  3.23  5.34  17.4     0     0     3     4       28.8
#>  9  15.5     8  318    150  2.76  3.52  16.9     0     0     3     2       18.8
#> 10  15.2     8  304    150  3.15  3.44  17.3     0     0     3     2       18.8
#> 11  13.3     8  350    245  3.73  3.84  15.4     0     0     3     4       30.6
#> 12  19.2     8  400    175  3.08  3.84  17.0     0     0     3     2       21.9
#> 13  15.8     8  351    264  4.22  3.17  14.5     0     1     5     4       33  
#> 14  15       8  301    335  3.54  3.57  14.6     0     1     5     8       41.9
#> # ℹ 1 more variable: high_performance <chr>

# View version history - old versions still accessible via time travel
list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-07-08 03:14:49              1
#> 2           7 2026-07-08 03:14:50              3
#> 3           8 2026-07-08 03:14:50              4
#> 4           9 2026-07-08 03:14:50              5
#>                                                                 changes
#> 1                    tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 3
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 3, 4
#> 4 tables_created, tables_dropped, tables_inserted_into, main.cars, 4, 5
#>          author                           commit_message commit_extra_info
#> 1 Data Engineer                    Initial car data load              <NA>
#> 2 Data Engineer       Update MPG for 4-cylinder vehicles              <NA>
#> 3 Data Engineer Add HP per cylinder and performance flag              <NA>
#> 4 Data Engineer                Filter to V8 engines only              <NA>
```

### Time Travel: Accessing Previous Versions

``` r

# Get the current version
current <- get_ducklake_table("cars") |> collect()

# List all snapshots to see available versions
snapshots <- list_table_snapshots("cars")
snapshots
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-07-08 03:14:49              1
#> 2           7 2026-07-08 03:14:50              3
#> 3           8 2026-07-08 03:14:50              4
#> 4           9 2026-07-08 03:14:50              5
#>                                                                 changes
#> 1                    tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 3
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 3, 4
#> 4 tables_created, tables_dropped, tables_inserted_into, main.cars, 4, 5
#>          author                           commit_message commit_extra_info
#> 1 Data Engineer                    Initial car data load              <NA>
#> 2 Data Engineer       Update MPG for 4-cylinder vehicles              <NA>
#> 3 Data Engineer Add HP per cylinder and performance flag              <NA>
#> 4 Data Engineer                Filter to V8 engines only              <NA>

# Access a specific previous version by snapshot_id
original_version <- get_ducklake_table_version(
  "cars", 
  snapshots$snapshot_id[1]
) |> collect()

# Compare: how many rows changed?
nrow(current)
#> [1] 14
nrow(original_version)
#> [1] 32
```
