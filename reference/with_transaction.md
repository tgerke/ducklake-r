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

  Optional author name to associate with the snapshot

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

## Examples

``` r
if (FALSE) { # \dontrun{
# Single operation
with_transaction(
  create_table(mtcars, "cars"),
  author = "Data Team",
  commit_message = "Add cars dataset"
)

# Multiple operations in a block
with_transaction({
  create_table(mtcars, "cars")
  create_table(iris, "flowers")
}, author = "Data Team", commit_message = "Add datasets")

# With dplyr pipeline
with_transaction(
  get_ducklake_table("cars") |>
    mutate(kpl = mpg * 0.425144) |>
    replace_table("cars"),
  author = "Data Team",
  commit_message = "Add km/L column"
)

# Automatic rollback on error
tryCatch(
  with_transaction({
    create_table(mtcars, "cars")
    stop("Simulated error")  # Transaction will be rolled back
  }),
  error = function(e) message("Transaction was rolled back: ", e$message)
)
} # }
```
