# Working with Transactions

``` r

library(ducklake)
library(dplyr)

# Setup for examples
attach_ducklake("transactions_lake", lake_path = vignette_temp_dir)
```

## Introduction

Transactions are essential for maintaining data integrity when making
multiple related changes to your data lake. DuckLake provides full ACID
(Atomicity, Consistency, Isolation, Durability) transaction support,
ensuring that either all operations succeed or none do.

DuckLake offers two approaches for working with transactions:

1.  **[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
    (Recommended)**: A modern, R-idiomatic approach that automatically
    handles errors and rollbacks
2.  **[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)
    /
    [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
    /
    [`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md)**:
    Manual transaction control for advanced use cases

This vignette demonstrates both approaches and explains when to use each
one.

## Setup: Loading Initial Data

We’ll use the `mtcars` dataset throughout this vignette to demonstrate
transaction workflows.

``` r

# Load initial data
with_transaction(
  create_table(mtcars, "cars"),
  author = "Tutorial",
  commit_message = "Initial load of mtcars dataset"
)
#> Committed snapshot 1 (Tutorial): Initial load of mtcars dataset

# View the data
get_ducklake_table("cars") |>
  select(mpg, cyl, hp, wt) |>
  head()
#> # A query:  ?? x 4
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpgXoPkb/ducklake/ducklake2e756dbdaa66.duckdb]
#>     mpg   cyl    hp    wt
#>   <dbl> <dbl> <dbl> <dbl>
#> 1  21       6   110  2.62
#> 2  21       6   110  2.88
#> 3  22.8     4    93  2.32
#> 4  21.4     6   110  3.22
#> 5  18.7     8   175  3.44
#> 6  18.1     6   105  3.46
```

## Approach 1: with_transaction() (Recommended)

The
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
function provides automatic error handling and cleanup, similar to the
[withr](https://withr.r-lib.org/) `with_*()` pattern used throughout the
R ecosystem. This is the **recommended approach** for most use cases.

### Why use with_transaction()?

- **Automatic rollback on error**: If any operation fails, all changes
  are automatically rolled back
- **Cleaner code**: No need to manually call
  [`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)
  and
  [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
- **Built-in metadata support**: Easily add author and commit messages
- **Safer**: Prevents accidentally leaving transactions open
- **R-idiomatic**: Follows familiar patterns from packages like `withr`

### Single Operation with Metadata

``` r

# Correct values in place, with automatic metadata tracking: the filter
# becomes the WHERE clause of a single UPDATE
with_transaction(
  get_ducklake_table("cars") |>
    filter(cyl == 4) |>
    mutate(mpg = mpg * 1.05) |>
    ducklake_exec(),
  author = "Data Team",
  commit_message = "Apply the revised 4-cylinder efficiency factor"
)
#> Committed snapshot 2 (Data Team): Apply the revised 4-cylinder efficiency
#> factor

# Verify the change
get_ducklake_table("cars") |>
  select(mpg, cyl) |>
  head()
#> # A query:  ?? x 2
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpgXoPkb/ducklake/ducklake2e756dbdaa66.duckdb]
#>     mpg   cyl
#>   <dbl> <dbl>
#> 1  21       6
#> 2  21       6
#> 3  21.4     6
#> 4  18.7     8
#> 5  18.1     6
#> 6  14.3     8
```

### Multiple Operations in a Single Transaction

You can group multiple operations together by wrapping them in curly
braces:

``` r

# Multiple related changes in one atomic transaction
with_transaction({
  # Add efficiency rating
  get_ducklake_table("cars") |>
    mutate(
      efficiency = case_when(
        mpg >= 25 ~ "high",
        mpg >= 20 ~ "medium",
        TRUE ~ "low"
      )
    ) |>
    replace_table("cars")
  
  # Create a summary table
  get_ducklake_table("cars") |>
    group_by(cyl) |>
    summarize(
      avg_mpg = mean(mpg, na.rm = TRUE),
      avg_hp = mean(hp, na.rm = TRUE),
      count = n()
    ) |>
    create_table("cars_summary")
}, author = "Data Team", commit_message = "Add efficiency ratings and summary table")
#> Committed snapshot 3 (Data Team): Add efficiency ratings and summary
#> table

# View results
get_ducklake_table("cars") |>
  select(mpg, cyl, efficiency) |>
  head()
#> # A query:  ?? x 3
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpgXoPkb/ducklake/ducklake2e756dbdaa66.duckdb]
#>     mpg   cyl efficiency
#>   <dbl> <dbl> <chr>     
#> 1  21       6 medium    
#> 2  21       6 medium    
#> 3  21.4     6 medium    
#> 4  18.7     8 low       
#> 5  18.1     6 low       
#> 6  14.3     8 low

get_ducklake_table("cars_summary") |>
  collect()
#> # A tibble: 3 × 4
#>     cyl avg_mpg avg_hp count
#>   <dbl>   <dbl>  <dbl> <dbl>
#> 1     4    28.0   82.6    11
#> 2     6    19.7  122.      7
#> 3     8    15.1  209.     14
```

### Automatic Rollback on Error

One of the key benefits of
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
is automatic error handling:

``` r

# This transaction will fail and automatically rollback
tryCatch(
  with_transaction({
    # This will succeed
    get_ducklake_table("cars") |>
      mutate(test_column = "temporary") |>
      replace_table("cars")
    
    # This will fail
    stop("Simulated error - something went wrong!")
  }, author = "Data Team", commit_message = "This will be rolled back"),
  error = function(e) {
    message("Transaction automatically rolled back: ", e$message)
  }
)
#> Transaction rolled back.
#> Transaction automatically rolled back: Transaction rolled back due to error: Simulated error - something went wrong!

# Verify that test_column was NOT added (transaction was rolled back)
get_ducklake_table("cars") |>
  colnames()
#>  [1] "mpg"        "cyl"        "disp"       "hp"         "drat"      
#>  [6] "wt"         "qsec"       "vs"         "am"         "gear"      
#> [11] "carb"       "efficiency"

# View all versioned changes
list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-09-08 15:14:25              1
#> 2           2 2026-09-08 15:14:25              1
#> 3           3 2026-09-08 15:14:25              2
#>                                                                                                       changes
#> 1                                                          tables_created, tables_inserted_into, main.cars, 1
#> 2                                                             tables_inserted_into, tables_deleted_from, 1, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, inlined_insert, main.cars, main.cars_summary, 1, 3, 2
#>      author                                 commit_message commit_extra_info
#> 1  Tutorial                 Initial load of mtcars dataset              <NA>
#> 2 Data Team Apply the revised 4-cylinder efficiency factor              <NA>
#> 3 Data Team       Add efficiency ratings and summary table              <NA>
```

## Approach 2: Manual Transaction Control

For advanced use cases where you need explicit control over transaction
boundaries, DuckLake provides manual transaction functions.

### When to use manual transactions?

- **Interactive workflows**: When you want to inspect data between
  operations before committing
- **Conditional commits**: When commit logic depends on runtime
  conditions
- **Long-running transactions**: When you need fine-grained control over
  transaction lifecycle
- **Legacy code**: When migrating from other transaction systems

### Basic Manual Transaction Workflow

``` r

# Start a transaction
begin_transaction()

# Make changes: declare a column, then fill it in place
add_table_column("cars", "weight_kg", "DOUBLE")
#> Added column "weight_kg" (DOUBLE) to "cars".
#> ℹ Metadata-only change; no data files were rewritten.
get_ducklake_table("cars") |>
  mutate(weight_kg = wt * 453.592) |>
  ducklake_exec()
#> [1] 32

# Commit the changes with metadata
commit_transaction(
  author = "Data Team",
  commit_message = "Add weight in kg"
)
#> Committed snapshot 4 (Data Team): Add weight in kg

# Verify changes
get_ducklake_table("cars") |>
  filter(cyl == 4) |>
  select(wt, weight_kg) |>
  head()
#> # A query:  ?? x 2
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpgXoPkb/ducklake/ducklake2e756dbdaa66.duckdb]
#>      wt weight_kg
#>   <dbl>     <dbl>
#> 1  2.32     1052.
#> 2  3.19     1447.
#> 3  3.15     1429.
#> 4  2.2       998.
#> 5  1.62      733.
#> 6  1.84      832.
```

### Manual Rollback

Sometimes you may want to inspect data before deciding whether to
commit:

``` r

# Start a transaction
begin_transaction()

# Make a test change
add_table_column("cars", "test_flag", "BOOLEAN", default = TRUE)
#> Added column "test_flag" (BOOLEAN) to "cars".
#> ℹ Metadata-only change; no data files were rewritten.

# Check the result
test_result <- get_ducklake_table("cars") |>
  select(mpg, test_flag) |>
  head() |>
  collect()

test_result
#> # A tibble: 6 × 2
#>     mpg test_flag
#>   <dbl> <lgl>    
#> 1  21   TRUE     
#> 2  21   TRUE     
#> 3  21.4 TRUE     
#> 4  18.7 TRUE     
#> 5  18.1 TRUE     
#> 6  14.3 TRUE

# Decide to rollback
rollback_transaction()
#> Transaction rolled back.

# Verify the change was NOT applied
"test_flag" %in% colnames(get_ducklake_table("cars"))
#> [1] FALSE

# View all versioned changes
list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 1           1 2026-09-08 15:14:25              1
#> 2           2 2026-09-08 15:14:25              1
#> 3           3 2026-09-08 15:14:25              2
#> 4           4 2026-09-08 15:14:26              3
#>                                                                                                       changes
#> 1                                                          tables_created, tables_inserted_into, main.cars, 1
#> 2                                                             tables_inserted_into, tables_deleted_from, 1, 1
#> 3 tables_created, tables_dropped, tables_inserted_into, inlined_insert, main.cars, main.cars_summary, 1, 3, 2
#> 4                                          tables_altered, tables_inserted_into, tables_deleted_from, 3, 3, 3
#>      author                                 commit_message commit_extra_info
#> 1  Tutorial                 Initial load of mtcars dataset              <NA>
#> 2 Data Team Apply the revised 4-cylinder efficiency factor              <NA>
#> 3 Data Team       Add efficiency ratings and summary table              <NA>
#> 4 Data Team                               Add weight in kg              <NA>
```

### Snapshot Metadata: At Commit Time vs After the Fact

DuckLake supports two ways to attach metadata (author, commit message,
and optional extra info) to a snapshot.

**At commit time (recommended)**: Pass `author`, `commit_message`,
and/or `commit_extra_info` to
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
or
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md).
This uses the DuckLake v1.0 `set_commit_message()` API to record
metadata as part of the transaction itself, before the commit is
finalized.

``` r

# Metadata set at commit time (preferred approach)
begin_transaction()

add_table_column("cars", "hp_per_liter", "DOUBLE")
#> Added column "hp_per_liter" (DOUBLE) to "cars".
#> ℹ Metadata-only change; no data files were rewritten.
get_ducklake_table("cars") |>
  mutate(hp_per_liter = hp / (cyl * 0.5)) |>
  ducklake_exec()
#> [1] 32

commit_transaction(
  author = "Performance Team",
  commit_message = "Add horsepower per liter metric",
  commit_extra_info = '{"ticket": "DATA-123"}'
)
#> Committed snapshot 5 (Performance Team): Add horsepower per liter metric

get_ducklake_table("cars") |>
  select(hp, cyl, hp_per_liter) |>
  head()
#> # A query:  ?? x 3
#> # Database: DuckDB 1.5.5 [unknown@Linux 6.17.0-1022-azure:R 4.6.1//tmp/RtmpgXoPkb/ducklake/ducklake2e756dbdaa66.duckdb]
#>      hp   cyl hp_per_liter
#>   <dbl> <dbl>        <dbl>
#> 1   110     6         36.7
#> 2   110     6         36.7
#> 3   110     6         36.7
#> 4   175     8         43.8
#> 5   105     6         35  
#> 6   245     8         61.2
```

**After the fact**:
[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
fills in the metadata of a snapshot that was committed without any, such
as a quick interactive change. It updates the
`ducklake_snapshot_changes` metadata table directly, outside DuckLake’s
transaction model, so by default it only fills empty fields and refuses
to replace a value that is already there:

``` r

# A quick interactive change, committed without metadata
begin_transaction()
rows_update(
  get_ducklake_table("cars"),
  data.frame(mpg = 21, hp_per_liter = 36.7),
  by = "mpg"
)
commit_transaction()
#> Committed snapshot 6.

# Fill in the metadata afterwards
set_snapshot_metadata(
  ducklake_name = "transactions_lake",
  author = "Performance Team",
  commit_message = "Correct hp_per_liter for the 21 mpg cars"
)
#> Snapshot metadata updated.

# Replacing an existing value takes overwrite = TRUE, and leaves no trace
# of the previous value
try(
  set_snapshot_metadata("transactions_lake", commit_message = "Reworded")
)
#> Error in set_snapshot_metadata("transactions_lake", commit_message = "Reworded") : 
#>   The latest snapshot already has commit_message set.
#> ℹ Metadata belongs on the commit: record it with `with_transaction()` or
#>   `commit_transaction()`.
#> ℹ Pass `overwrite = TRUE` to replace it; the previous value is not kept.
```

Both approaches populate the same fields. Set metadata at commit time
whenever you can: it is recorded inside the transaction, by DuckLake
itself. Keep
[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
for the occasional snapshot that was committed without it. Where the
snapshot history is the audit trail, treat `overwrite = TRUE` as what it
is: an edit to that trail.

## Viewing Transaction History

Regardless of which approach you use, all transactions are tracked with
complete metadata:

``` r

# View recent transaction history
list_table_snapshots("cars") |>
  select(snapshot_id, snapshot_time, author, commit_message) |>
  tail(5)
#>   snapshot_id       snapshot_time           author
#> 2           2 2026-09-08 15:14:25        Data Team
#> 3           3 2026-09-08 15:14:25        Data Team
#> 4           4 2026-09-08 15:14:26        Data Team
#> 5           5 2026-09-08 15:14:26 Performance Team
#> 6           6 2026-09-08 15:14:26 Performance Team
#>                                   commit_message
#> 2 Apply the revised 4-cylinder efficiency factor
#> 3       Add efficiency ratings and summary table
#> 4                               Add weight in kg
#> 5                Add horsepower per liter metric
#> 6       Correct hp_per_liter for the 21 mpg cars
```

## Working with several writers

When a lake has more than one writer (a PostgreSQL or SQLite catalog
shared by several sessions, or a Quack server), two transactions can
race to commit the next snapshot. DuckLake resolves the race itself: the
transaction that lost checks whether its changes conflict with the
winner’s, and when they do not, it is retried against the new snapshot.
Two sessions appending to the same table, or editing different tables,
both commit. Real conflicts surface as an error in the losing session,
whose changes are rolled back: two transactions deleting from the same
data file, an insert into a table the other transaction dropped or
altered, or two creates of the same name. Re-run that transaction on the
current data.

The retry policy is a per-session DuckDB setting: 10 attempts, 100 ms
apart, with the wait growing by 1.5 each time.
[`set_ducklake_retry()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_retry.md)
adjusts it for a busy catalog:

``` r

set_ducklake_retry(max_retries = 20, wait_ms = 250, backoff = 2)
```

## Comparison: with_transaction() vs Manual Control

| Feature | [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md) | Manual (`begin/commit/rollback`) |
|----|----|----|
| **Ease of use** | ✅ Simple, one function | ❌ Requires multiple function calls |
| **Error handling** | ✅ Automatic rollback | ❌ Must handle manually |
| **Metadata** | ✅ Inline with transaction | ✅ Inline via parameter, or filled in later via [`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md) |
| **Safety** | ✅ Can’t forget to commit | ❌ Risk of open transactions |
| **Use case** | Most production workflows | Interactive/conditional workflows |
| **Code clarity** | ✅ Clear transaction scope | ⚠ Scope can be unclear |

## Best Practices

1.  **Default to
    [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)**:
    Use it for all standard workflows
2.  **Always add metadata**: Include `author` and `commit_message` for
    audit trails
3.  **Keep transactions focused**: Group related changes, but avoid
    overly long transactions; when other sessions write to the same
    lake, a long-open transaction is the one most likely to conflict
4.  **Handle errors gracefully**: When using manual transactions, always
    use [`tryCatch()`](https://rdrr.io/r/base/conditions.html) to ensure
    rollback
5.  **Test rollback behavior**: Verify that your error handling works
    correctly

## Key Concepts Summary

- **`with_transaction(expr, author, commit_message, commit_extra_info)`**:
  Modern, automatic transaction handling (recommended)
- **[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)**:
  Start a manual transaction
- **`commit_transaction(author, commit_message, commit_extra_info)`**:
  Apply changes from a manual transaction, with optional metadata set at
  commit time via the DuckLake v1.0 API
- **[`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md)**:
  Discard changes from a manual transaction
- **[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)**:
  Fill in metadata the most recent snapshot was committed without
  (`overwrite = TRUE` to replace a value)

Transactions ensure data integrity and provide complete audit trails for
all changes in your DuckLake.
