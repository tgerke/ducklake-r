# Modifying Tables with Version Control

This vignette demonstrates how to modify tables in a DuckLake while
maintaining complete version control and audit trails. This is essential
for reproducible workflows.

``` r

library(ducklake)
library(dplyr)
#> 
#> Attaching package: 'dplyr'
#> The following objects are masked from 'package:ducklake':
#> 
#>     rows_delete, rows_insert, rows_update
#> The following objects are masked from 'package:stats':
#> 
#>     filter, lag
#> The following objects are masked from 'package:base':
#> 
#>     intersect, setdiff, setequal, union

# Setup for examples
install_ducklake()
#> Installed ducklake extension.
attach_ducklake("modifying_tables_lake", lake_path = vignette_temp_dir)

# Load a sample dataset
with_transaction(
  create_table(mtcars, "cars"),
  author = "Data Engineer",
  commit_message = "Initial car data load"
)
#> Transaction started.
#> Transaction committed.
```

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
#> 2           1 2026-07-07 23:17:01              1
#> 3           2 2026-07-07 23:17:01              2
#>                                                                 changes
#> 2                    tables_created, tables_inserted_into, main.cars, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#>          author                     commit_message commit_extra_info
#> 2 Data Engineer              Initial car data load              <NA>
#> 3 Data Engineer Update MPG for 4-cylinder vehicles              <NA>
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
#> # Database: DuckDB 1.5.4 [unknown@Linux 6.17.0-1018-azure:R 4.6.1//tmp/RtmpB8In0W/ducklake/ducklake1f2958ae6671.duckdb]
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
#> # Database: DuckDB 1.5.4 [unknown@Linux 6.17.0-1018-azure:R 4.6.1//tmp/RtmpB8In0W/ducklake/ducklake1f2958ae6671.duckdb]
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
#> 2           1 2026-07-07 23:17:01              1
#> 3           2 2026-07-07 23:17:01              2
#> 4           3 2026-07-07 23:17:02              3
#> 5           4 2026-07-07 23:17:02              4
#>                                                                 changes
#> 2                    tables_created, tables_inserted_into, main.cars, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 4 tables_created, tables_dropped, tables_inserted_into, main.cars, 2, 3
#> 5 tables_created, tables_dropped, tables_inserted_into, main.cars, 3, 4
#>          author                           commit_message commit_extra_info
#> 2 Data Engineer                    Initial car data load              <NA>
#> 3 Data Engineer       Update MPG for 4-cylinder vehicles              <NA>
#> 4 Data Engineer Add HP per cylinder and performance flag              <NA>
#> 5 Data Engineer                Filter to V8 engines only              <NA>
```

### Time Travel: Accessing Previous Versions

``` r

# Get the current version
current <- get_ducklake_table("cars") |> collect()

# List all snapshots to see available versions
snapshots <- list_table_snapshots("cars")
snapshots
#>   snapshot_id       snapshot_time schema_version
#> 2           1 2026-07-07 23:17:01              1
#> 3           2 2026-07-07 23:17:01              2
#> 4           3 2026-07-07 23:17:02              3
#> 5           4 2026-07-07 23:17:02              4
#>                                                                 changes
#> 2                    tables_created, tables_inserted_into, main.cars, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 4 tables_created, tables_dropped, tables_inserted_into, main.cars, 2, 3
#> 5 tables_created, tables_dropped, tables_inserted_into, main.cars, 3, 4
#>          author                           commit_message commit_extra_info
#> 2 Data Engineer                    Initial car data load              <NA>
#> 3 Data Engineer       Update MPG for 4-cylinder vehicles              <NA>
#> 4 Data Engineer Add HP per cylinder and performance flag              <NA>
#> 5 Data Engineer                Filter to V8 engines only              <NA>

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
