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
The choice between the styles is about *what kind of change* you are
making, not about whether it is audited.

One distinction matters before anything else: dplyr joins **read** –
[`left_join()`](https://dplyr.tidyverse.org/reference/mutate-joins.html)
and friends combine tables into a new result and never touch the stored
data. Everything else in this table **writes**.

| You want to… | Reach for |
|----|----|
| Look up or combine data for analysis | dplyr joins ([`left_join()`](https://dplyr.tidyverse.org/reference/mutate-joins.html), …) |
| Append, correct, or remove specific rows | [`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md), [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md), [`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md) |
| Update rows that exist, insert the ones that don’t | [`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md) |
| Conditional updates, or deletes driven by a staging table | [`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md) |
| Change the schema, not the data | [`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md) and the schema evolution family |
| Bulk transformations that touch most rows | [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md) |

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

### For update-or-insert: `rows_upsert()`

When a batch mixes corrections to existing rows with rows you have never
seen,
[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)
handles both in one atomic statement: rows whose key matches are
updated, the rest are inserted.

``` r

# One statement, one snapshot: id 2 is updated, id 7 is inserted
rows_upsert(get_ducklake_table("my_table"), mixed_batch, by = "id")
```

DuckLake tables have no primary keys, so under the hood this is SQL
`MERGE INTO` matching on your `by` columns, not the `ON CONFLICT` upsert
you may know from other databases. The practical upshot is the same,
with one wrinkle: when the upsert data covers only some of the table’s
columns, *inserted* rows get the column default (usually `NULL`) in the
columns you didn’t supply, while *updated* rows keep their existing
values there.

### For staging syncs and conditional merges: `merge_into()`

[`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)
covers update-or-insert.
[`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md)
exposes the rest of the SQL MERGE statement for the cases beyond it:

``` r

# Update only when the source is newer
merge_into(
  get_ducklake_table("my_table"), fresh_data, by = "id",
  matched_condition = "source.updated_at > target.updated_at"
)

# Synchronize to a staging table: upsert, plus delete rows
# that no longer exist in the source
merge_into("my_table", staging, by = "id", delete_missing = TRUE)
```

Despite the name,
[`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md)
is unrelated to [`base::merge()`](https://rdrr.io/r/base/merge.html) and
dplyr’s joins, which combine tables into a new result.
[`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md)
changes the target table in place.

You could get a similar end state by joining the staging table to the
current table and calling
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
on the result. Resist that instinct: it rewrites every row of the table,
and the change feed then records a wholesale replacement instead of the
handful of inserts, updates, and deletes that actually happened.
[`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md)
touches only the affected rows, so
[`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md)
afterward tells the true story.

### For schema changes: evolve in place

Adding, dropping, renaming, or widening columns needs no data rewrite at
all. DuckLake records schema changes as metadata, so the schema
evolution functions run instantly regardless of table size, and time
travel still shows every earlier shape of the table:

``` r

add_table_column("my_table", "category", "VARCHAR", default = "unknown")
rename_table_column("my_table", from = "cat", to = "category")
set_column_type("my_table", "id", "BIGINT")   # widening only
drop_table_column("my_table", "scratch")
rename_ducklake_table("my_table", "my_better_named_table")
```

Two behaviors to know. A `default` applies to existing rows as well as
future inserts, so the new column shows up filled everywhere. And type
changes must widen (`INTEGER` to `BIGINT`, `FLOAT` to `DOUBLE`):
DuckLake refuses conversions that could lose values, and the error from
[`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)
walks you through the add-copy-drop-rename route when you need to
narrow.

### For structural or bulk changes: `replace_table()`

Use
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
when a transformation recomputes, reshapes, or filters most of the table
anyway:

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
collects the transformed data into R and rewrites the table – the right
tool for a bulk rewrite, wasteful for touching three rows in a
million-row table, and unnecessary for pure schema changes now that the
in-place functions above exist.

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
#> 1           2 2026-08-10 19:09:02              2
#> 2           3 2026-08-10 19:09:02              2
#> 3           4 2026-08-10 19:09:02              2
#> 4           5 2026-08-10 19:09:03              2
#> 5           6 2026-08-10 19:09:03              2
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

**Upsert** a batch that mixes corrections and new arrivals. The Model
3’s mileage is updated and the Kona is inserted, in one statement:

``` r

service_batch <- data.frame(
  car_id = c(3, 7),
  model = c("Model 3", "Kona"),
  mileage = c(15200, 8000)
)

rows_upsert(get_ducklake_table("fleet"), service_batch, by = "car_id")

get_ducklake_table("fleet") |> arrange(car_id) |> collect()
#> # A tibble: 5 × 3
#>   car_id model   mileage
#>    <int> <chr>     <dbl>
#> 1      2 Civic     39000
#> 2      3 Model 3   15200
#> 3      5 Ioniq 5     120
#> 4      6 ID.4         60
#> 5      7 Kona       8000
```

**Synchronize** to an authoritative source with
[`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md).
Suppose the quarterly registry export is the truth: matching cars take
its values, cars it doesn’t list are gone, and cars we haven’t seen are
added. One call, one snapshot:

``` r

registry <- data.frame(
  car_id = c(3, 5, 8),
  model = c("Model 3", "Ioniq 5", "e-Golf"),
  mileage = c(15400, 900, 21000)
)

with_transaction(
  merge_into("fleet", registry, by = "car_id", delete_missing = TRUE),
  author = "Fleet Manager",
  commit_message = "Quarterly registry sync"
)
#> Transaction started.
#> Transaction committed.

get_ducklake_table("fleet") |> arrange(car_id) |> collect()
#> # A tibble: 3 × 3
#>   car_id model   mileage
#>    <int> <chr>     <dbl>
#> 1      3 Model 3   15400
#> 2      5 Ioniq 5     900
#> 3      8 e-Golf    21000
```

Because the sync ran as targeted row changes rather than a table
rewrite, the change feed records exactly what happened:

``` r

latest <- max(list_table_snapshots("fleet")$snapshot_id)
get_table_changes("fleet", latest, latest) |>
  select(change_type, car_id, model, mileage) |>
  collect()
#> # A tibble: 8 × 4
#>   change_type      car_id model   mileage
#>   <chr>             <int> <chr>     <dbl>
#> 1 insert                8 e-Golf    21000
#> 2 update_postimage      3 Model 3   15400
#> 3 update_postimage      5 Ioniq 5     900
#> 4 delete                7 Kona       8000
#> 5 update_preimage       5 Ioniq 5     120
#> 6 update_preimage       3 Model 3   15200
#> 7 delete                2 Civic     39000
#> 8 delete                6 ID.4         60
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
#> 1           1 2026-08-10 19:09:02              1
#> 2           9 2026-08-10 19:09:03              3
#>                                                                 changes
#> 1                    tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 3
#>          author                     commit_message commit_extra_info
#> 1 Data Engineer              Initial car data load              <NA>
#> 2 Data Engineer Update MPG for 4-cylinder vehicles              <NA>
```

### Adding derived columns

Derived columns no longer need
[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md).
Declare the column with
[`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)
(instant, metadata-only), then fill it with a
[`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html) pipeline
through
[`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
which runs as an in-database UPDATE – nothing is collected into R:

``` r

with_transaction({
  add_table_column("cars", "hp_per_cyl", "DOUBLE")
  add_table_column("cars", "high_performance", "VARCHAR")

  get_ducklake_table("cars") |>
    mutate(
      hp_per_cyl = hp / cyl,
      high_performance = if_else(hp > 200, "Y", "N")
    ) |>
    ducklake_exec()
},
  author = "Data Engineer",
  commit_message = "Add HP per cylinder and performance flag"
)
#> Transaction started.
#> Added column "hp_per_cyl" (DOUBLE) to "cars".
#> ℹ Metadata-only change; no data files were rewritten.
#> Added column "high_performance" (VARCHAR) to "cars".
#> ℹ Metadata-only change; no data files were rewritten.
#> Transaction committed.

# Verify new columns exist
get_ducklake_table("cars") |>
  filter(hp > 200) |>
  select(hp, cyl, hp_per_cyl, high_performance)
#> # A query:  ?? x 4
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1020-azure:R 4.6.1//tmp/RtmpFfG0ep/ducklake/ducklake22c9773e29de.duckdb]
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

### Reshaping the schema in place

The rest of the schema evolution family works the same way. Widen a
type, rename a column, drop one – each change is instant, and earlier
snapshots keep the earlier shape:

``` r

snapshot_before <- max(list_table_snapshots("cars")$snapshot_id)

rename_table_column("cars", from = "high_performance", to = "high_perf_flag")
#> Renamed column "high_performance" to "high_perf_flag" in
#> "cars".
drop_table_column("cars", "hp_per_cyl")
#> Dropped column "hp_per_cyl" from "cars". Earlier
#> snapshots still contain it.

# Widen fleet's integer key without rewriting any data
set_column_type("fleet", "car_id", "BIGINT")
#> Column "car_id" in "fleet" is now BIGINT.

# Current schema reflects the rename and the drop
get_ducklake_table("cars") |> colnames()
#>  [1] "mpg"            "cyl"            "disp"           "hp"            
#>  [5] "drat"           "wt"             "qsec"           "vs"            
#>  [9] "am"             "gear"           "carb"           "high_perf_flag"

# The pre-change snapshot still shows the old shape
get_ducklake_table_version("cars", snapshot_before) |> colnames()
#>  [1] "mpg"              "cyl"              "disp"             "hp"              
#>  [5] "drat"             "wt"               "qsec"             "vs"              
#>  [9] "am"               "gear"             "carb"             "hp_per_cyl"      
#> [13] "high_performance"
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
#> # A query:  ?? x 12
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1020-azure:R 4.6.1//tmp/RtmpFfG0ep/ducklake/ducklake22c9773e29de.duckdb]
#>      mpg   cyl  disp    hp  drat    wt  qsec    vs    am  gear  carb
#>    <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl> <dbl>
#>  1  18.7     8  360    175  3.15  3.44  17.0     0     0     3     2
#>  2  14.3     8  360    245  3.21  3.57  15.8     0     0     3     4
#>  3  16.4     8  276.   180  3.07  4.07  17.4     0     0     3     3
#>  4  17.3     8  276.   180  3.07  3.73  17.6     0     0     3     3
#>  5  15.2     8  276.   180  3.07  3.78  18       0     0     3     3
#>  6  10.4     8  472    205  2.93  5.25  18.0     0     0     3     4
#>  7  10.4     8  460    215  3     5.42  17.8     0     0     3     4
#>  8  14.7     8  440    230  3.23  5.34  17.4     0     0     3     4
#>  9  15.5     8  318    150  2.76  3.52  16.9     0     0     3     2
#> 10  15.2     8  304    150  3.15  3.44  17.3     0     0     3     2
#> 11  13.3     8  350    245  3.73  3.84  15.4     0     0     3     4
#> 12  19.2     8  400    175  3.08  3.84  17.0     0     0     3     2
#> 13  15.8     8  351    264  4.22  3.17  14.5     0     1     5     4
#> 14  15       8  301    335  3.54  3.57  14.6     0     1     5     8
#> # ℹ 1 more variable: high_perf_flag <chr>

# View version history - old versions still accessible via time travel
list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-08-10 19:09:02              1
#> 2           9 2026-08-10 19:09:03              3
#> 3          10 2026-08-10 19:09:04              4
#> 4          11 2026-08-10 19:09:04              5
#> 5          12 2026-08-10 19:09:04              6
#> 6          14 2026-08-10 19:09:04              8
#>                                                                 changes
#> 1                    tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 3
#> 3    tables_altered, tables_inserted_into, tables_deleted_from, 3, 3, 3
#> 4                                                     tables_altered, 3
#> 5                                                     tables_altered, 3
#> 6 tables_created, tables_dropped, tables_inserted_into, main.cars, 3, 4
#>          author                           commit_message commit_extra_info
#> 1 Data Engineer                    Initial car data load              <NA>
#> 2 Data Engineer       Update MPG for 4-cylinder vehicles              <NA>
#> 3 Data Engineer Add HP per cylinder and performance flag              <NA>
#> 4          <NA>                                     <NA>              <NA>
#> 5          <NA>                                     <NA>              <NA>
#> 6 Data Engineer                Filter to V8 engines only              <NA>
```

### Time Travel: Accessing Previous Versions

``` r

# Get the current version
current <- get_ducklake_table("cars") |> collect()

# List all snapshots to see available versions
snapshots <- list_table_snapshots("cars")
snapshots
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-08-10 19:09:02              1
#> 2           9 2026-08-10 19:09:03              3
#> 3          10 2026-08-10 19:09:04              4
#> 4          11 2026-08-10 19:09:04              5
#> 5          12 2026-08-10 19:09:04              6
#> 6          14 2026-08-10 19:09:04              8
#>                                                                 changes
#> 1                    tables_created, tables_inserted_into, main.cars, 1
#> 2 tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 3
#> 3    tables_altered, tables_inserted_into, tables_deleted_from, 3, 3, 3
#> 4                                                     tables_altered, 3
#> 5                                                     tables_altered, 3
#> 6 tables_created, tables_dropped, tables_inserted_into, main.cars, 3, 4
#>          author                           commit_message commit_extra_info
#> 1 Data Engineer                    Initial car data load              <NA>
#> 2 Data Engineer       Update MPG for 4-cylinder vehicles              <NA>
#> 3 Data Engineer Add HP per cylinder and performance flag              <NA>
#> 4          <NA>                                     <NA>              <NA>
#> 5          <NA>                                     <NA>              <NA>
#> 6 Data Engineer                Filter to V8 engines only              <NA>

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
