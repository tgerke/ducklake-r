# Begin a transaction

Starts a new transaction in the DuckDB connection. All subsequent
operations will be part of this transaction until it is committed or
rolled back.

## Usage

``` r
begin_transaction(conn = NULL)
```

## Arguments

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

Invisibly returns TRUE on success

## Details

Transactions allow you to group multiple operations together and ensure
they either all succeed or all fail. Use
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
to apply the changes or
[`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md)
to discard them.

DuckDB supports full ACID transactions with multiple isolation levels.

## Examples

``` r
if (FALSE) { # \dontrun{
# Start a transaction
begin_transaction()

# Make some changes
get_ducklake_table("my_table") |>
  filter(status == "pending") |>
  mutate(status = "processed") |>
  ducklake_exec()

# Commit if everything looks good
commit_transaction()

# Or rollback if something went wrong
# rollback_transaction()
} # }
```
