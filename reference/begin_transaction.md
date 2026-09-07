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

## See also

Other transactions:
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md),
[`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md),
[`set_ducklake_retry()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_retry.md),
[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md),
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)

## Examples

``` r
lake_dir <- tempfile("begin_lake_")
dir.create(lake_dir)
attach_ducklake("begin_lake", lake_path = lake_dir)
create_table(data.frame(id = 1:3, status = "pending"), "jobs")

# Start a transaction
begin_transaction()

# Make some changes
get_ducklake_table("jobs") |>
  dplyr::filter(status == "pending") |>
  dplyr::mutate(status = "processed") |>
  ducklake_exec()
#> [1] 3

# Commit if everything looks good
commit_transaction()
#> Committed snapshot 2.

# Or rollback if something went wrong
# rollback_transaction()

detach_ducklake("begin_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
