# Execute code within a transaction

Wraps code execution in a transaction, automatically committing on
success or rolling back on error. This provides a more R-idiomatic and
safer way to handle transactions compared to manually calling
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)
and
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md).

## Usage

``` r
with_transaction(
  expr,
  author = NULL,
  commit_message = NULL,
  commit_extra_info = NULL,
  conn = NULL
)
```

## Arguments

- expr:

  An R expression or code block to execute within the transaction. Can
  be a single statement or a `{...}` block containing multiple
  statements.

- author:

  Author to record on the snapshot. Defaults to the `ducklake.author`
  option when it is set (see
  [`?ducklake`](https://tgerke.github.io/ducklake-r/reference/ducklake-package.md)),
  otherwise none.

- commit_message:

  Optional commit message describing the changes

- commit_extra_info:

  Optional extra information about the commit

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

Invisibly returns the result of the expression

## Details

This function provides automatic error handling and cleanup for
transactions:

- Begins a transaction before executing the code

- Executes the provided expression

- On success: commits the transaction and adds metadata (if provided)

- On error: automatically rolls back the transaction and re-throws the
  error

This pattern is similar to `withr::with_*()` functions and provides
better safety guarantees than manually managing transactions.

## See also

Other transactions:
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md),
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md),
[`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md),
[`set_ducklake_retry()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_retry.md),
[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)

## Examples

``` r
lake_dir <- tempfile("with_tx_lake_")
dir.create(lake_dir)
attach_ducklake("with_tx_lake", lake_path = lake_dir)

# Single operation
with_transaction(
  create_table(mtcars, "cars"),
  author = "Data Team",
  commit_message = "Add cars dataset"
)
#> Committed snapshot 1 (Data Team): Add cars dataset

# Multiple operations in a block
with_transaction({
  create_table(iris, "flowers")
  create_table(airquality, "air")
}, author = "Data Team", commit_message = "Add datasets")
#> Converted factor column Species to character (DuckLake does not support ENUM
#> columns).
#> Committed snapshot 2 (Data Team): Add datasets

# With dplyr pipeline
with_transaction(
  get_ducklake_table("cars") |>
    dplyr::mutate(kpl = mpg * 0.425144) |>
    replace_table("cars"),
  author = "Data Team",
  commit_message = "Add km/L column"
)
#> Committed snapshot 3 (Data Team): Add km/L column

# Automatic rollback on error
tryCatch(
  with_transaction({
    create_table(ChickWeight, "chicks")
    stop("Simulated error") # Transaction will be rolled back
  }),
  error = function(e) message("Transaction was rolled back: ", e$message)
)
#> Converted factor columns Chick and Diet to character (DuckLake does not support
#> ENUM columns).
#> Transaction rolled back.
#> Transaction was rolled back: Transaction rolled back due to error: Simulated error

# "chicks" was never committed
list_ducklake_tables()
#>   schema_name table_name  type
#> 1        main        air table
#> 2        main       cars table
#> 3        main    flowers table

detach_ducklake("with_tx_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
