# Working with Transactions

``` r
library(ducklake)
library(dplyr)

# Setup for examples
install_ducklake()
attach_ducklake("my_ducklake", lake_path = tempdir())
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
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# View the data
get_ducklake_table("cars") |>
  select(mpg, cyl, hp, wt) |>
  head()
#> # Source:   SQL [?? x 4]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmprYJz5b/duckplyr/duckplyr218268dbbade.duckdb]
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
`withr::with_*()` pattern used throughout the R ecosystem. This is the
**recommended approach** for most use cases.

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
# Add a new column with automatic metadata tracking
with_transaction(
  get_ducklake_table("cars") |>
    mutate(kpl = mpg * 0.425144) |>
    replace_table("cars"),
  author = "Data Team",
  commit_message = "Add kilometers per liter column"
)
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# Verify the change
get_ducklake_table("cars") |>
  select(mpg, kpl) |>
  head()
#> # Source:   SQL [?? x 2]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmprYJz5b/duckplyr/duckplyr218268dbbade.duckdb]
#>     mpg   kpl
#>   <dbl> <dbl>
#> 1  21    8.93
#> 2  21    8.93
#> 3  22.8  9.69
#> 4  21.4  9.10
#> 5  18.7  7.95
#> 6  18.1  7.70
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
#> Transaction started
#> Transaction committed
#> Snapshot metadata updated

# View results
get_ducklake_table("cars") |>
  select(mpg, cyl, efficiency) |>
  head()
#> # Source:   SQL [?? x 3]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmprYJz5b/duckplyr/duckplyr218268dbbade.duckdb]
#>     mpg   cyl efficiency
#>   <dbl> <dbl> <chr>     
#> 1  21       6 medium    
#> 2  21       6 medium    
#> 3  22.8     4 medium    
#> 4  21.4     6 medium    
#> 5  18.7     8 low       
#> 6  18.1     6 low

get_ducklake_table("cars_summary") |>
  collect()
#> # A tibble: 3 × 4
#>     cyl avg_mpg avg_hp count
#>   <dbl>   <dbl>  <dbl> <dbl>
#> 1     6    19.7  122.      7
#> 2     8    15.1  209.     14
#> 3     4    26.7   82.6    11
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
#> Transaction started
#> Transaction rolled back
#> Transaction automatically rolled back: Transaction rolled back due to error: Simulated error - something went wrong!

# Verify that test_column was NOT added (transaction was rolled back)
get_ducklake_table("cars") |>
  colnames()
#>  [1] "mpg"        "cyl"        "disp"       "hp"         "drat"      
#>  [6] "wt"         "qsec"       "vs"         "am"         "gear"      
#> [11] "carb"       "kpl"        "efficiency"

# View all versioned changes
list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 2           1 2026-02-09 17:35:33              1
#> 3           2 2026-02-09 17:35:33              2
#> 4           3 2026-02-09 17:35:34              3
#>                                                                                       changes
#> 2                                          tables_created, tables_inserted_into, main.cars, 1
#> 3                       tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 4 tables_created, tables_dropped, tables_inserted_into, main.cars, main.cars_summary, 2, 3, 4
#>      author                           commit_message commit_extra_info
#> 2  Tutorial           Initial load of mtcars dataset              <NA>
#> 3 Data Team          Add kilometers per liter column              <NA>
#> 4 Data Team Add efficiency ratings and summary table              <NA>
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
#> Transaction started

# Make changes
get_ducklake_table("cars") |>
  filter(cyl == 4) |>
  mutate(weight_kg = wt * 453.592) |>
  replace_table("cars")

# Commit the changes with metadata
commit_transaction(
  author = "Data Team",
  commit_message = "Add weight in kg for 4-cylinder cars"
)
#> Transaction committed
#> Snapshot metadata updated

# Verify changes
get_ducklake_table("cars") |>
  filter(cyl == 4) |>
  select(wt, weight_kg) |>
  head()
#> # Source:   SQL [?? x 2]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmprYJz5b/duckplyr/duckplyr218268dbbade.duckdb]
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
#> Transaction started

# Make a test change
get_ducklake_table("cars") |>
  mutate(test_flag = TRUE) |>
  replace_table("cars")

# Check the result
test_result <- get_ducklake_table("cars") |>
  select(mpg, test_flag) |>
  head() |>
  collect()

print(test_result)
#> # A tibble: 6 × 2
#>     mpg test_flag
#>   <dbl> <lgl>    
#> 1  22.8 TRUE     
#> 2  24.4 TRUE     
#> 3  22.8 TRUE     
#> 4  32.4 TRUE     
#> 5  30.4 TRUE     
#> 6  33.9 TRUE

# Decide to rollback
rollback_transaction()
#> Transaction rolled back

# Verify the change was NOT applied
"test_flag" %in% colnames(get_ducklake_table("cars"))
#> [1] FALSE

# View all versioned changes
list_table_snapshots("cars")
#>   snapshot_id       snapshot_time schema_version
#> 2           1 2026-02-09 17:35:33              1
#> 3           2 2026-02-09 17:35:33              2
#> 4           3 2026-02-09 17:35:34              3
#> 5           4 2026-02-09 17:35:34              4
#>                                                                                       changes
#> 2                                          tables_created, tables_inserted_into, main.cars, 1
#> 3                       tables_created, tables_dropped, tables_inserted_into, main.cars, 1, 2
#> 4 tables_created, tables_dropped, tables_inserted_into, main.cars, main.cars_summary, 2, 3, 4
#> 5                       tables_created, tables_dropped, tables_inserted_into, main.cars, 4, 5
#>      author                           commit_message commit_extra_info
#> 2  Tutorial           Initial load of mtcars dataset              <NA>
#> 3 Data Team          Add kilometers per liter column              <NA>
#> 4 Data Team Add efficiency ratings and summary table              <NA>
#> 5 Data Team     Add weight in kg for 4-cylinder cars              <NA>
```

### Setting Metadata Separately

With manual transactions, you can also set metadata after committing:

``` r
# Start transaction
begin_transaction()
#> Transaction started

# Make changes
get_ducklake_table("cars") |>
  mutate(hp_per_liter = hp / (cyl * 0.5)) |>
  replace_table("cars")

# Commit
commit_transaction()
#> Transaction committed

# Add metadata separately
set_snapshot_metadata(
  ducklake_name = "my_ducklake",
  author = "Performance Team",
  commit_message = "Add horsepower per liter metric"
)
#> Snapshot metadata updated

# Verify
get_ducklake_table("cars") |>
  select(hp, cyl, hp_per_liter) |>
  head()
#> # Source:   SQL [?? x 3]
#> # Database: DuckDB 1.4.4 [unknown@Linux 6.11.0-1018-azure:R 4.5.2//tmp/RtmprYJz5b/duckplyr/duckplyr218268dbbade.duckdb]
#>      hp   cyl hp_per_liter
#>   <dbl> <dbl>        <dbl>
#> 1    93     4         46.5
#> 2    62     4         31  
#> 3    95     4         47.5
#> 4    66     4         33  
#> 5    52     4         26  
#> 6    65     4         32.5
```

## Viewing Transaction History

Regardless of which approach you use, all transactions are tracked with
complete metadata:

``` r
# View recent transaction history
list_table_snapshots("cars") |>
  select(snapshot_id, snapshot_time, author, commit_message) |>
  tail(5)
#>   snapshot_id       snapshot_time           author
#> 2           1 2026-02-09 17:35:33         Tutorial
#> 3           2 2026-02-09 17:35:33        Data Team
#> 4           3 2026-02-09 17:35:34        Data Team
#> 5           4 2026-02-09 17:35:34        Data Team
#> 6           5 2026-02-09 17:35:35 Performance Team
#>                             commit_message
#> 2           Initial load of mtcars dataset
#> 3          Add kilometers per liter column
#> 4 Add efficiency ratings and summary table
#> 5     Add weight in kg for 4-cylinder cars
#> 6          Add horsepower per liter metric
```

## Comparison: with_transaction() vs Manual Control

| Feature            | [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md) | Manual (`begin/commit/rollback`)       |
|--------------------|-------------------------------------------------------------------------------------------|----------------------------------------|
| **Ease of use**    | ✅ Simple, one function                                                                   | ❌ Requires multiple function calls    |
| **Error handling** | ✅ Automatic rollback                                                                     | ❌ Must handle manually                |
| **Metadata**       | ✅ Inline with transaction                                                                | ⚠️ Requires separate call or parameter |
| **Safety**         | ✅ Can’t forget to commit                                                                 | ❌ Risk of open transactions           |
| **Use case**       | Most production workflows                                                                 | Interactive/conditional workflows      |
| **Code clarity**   | ✅ Clear transaction scope                                                                | ⚠️ Scope can be unclear                |

## Best Practices

1.  **Default to
    [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)**:
    Use it for all standard workflows
2.  **Always add metadata**: Include `author` and `commit_message` for
    audit trails
3.  **Keep transactions focused**: Group related changes, but avoid
    overly long transactions
4.  **Handle errors gracefully**: When using manual transactions, always
    use [`tryCatch()`](https://rdrr.io/r/base/conditions.html) to ensure
    rollback
5.  **Test rollback behavior**: Verify that your error handling works
    correctly

## Key Concepts Summary

- **`with_transaction(expr, author, commit_message)`**: Modern,
  automatic transaction handling (recommended)
- **[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)**:
  Start a manual transaction
- **`commit_transaction(author, commit_message)`**: Apply changes from a
  manual transaction
- **[`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md)**:
  Discard changes from a manual transaction
- **[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)**:
  Add metadata to snapshots after committing

Transactions ensure data integrity and provide complete audit trails for
all changes in your DuckLake.
