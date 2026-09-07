# Rollback a transaction

Rolls back the current transaction, discarding all changes made since
the transaction began.

## Usage

``` r
rollback_transaction(conn = NULL)
```

## Arguments

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

Invisibly returns TRUE on success

## Details

This function discards all changes made since
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)
was called, reverting the database to its state before the transaction
began.

## See also

Other transactions:
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md),
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md),
[`set_ducklake_retry()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_retry.md),
[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md),
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)

## Examples

``` r
lake_dir <- tempfile("rollback_lake_")
dir.create(lake_dir)
attach_ducklake("rollback_lake", lake_path = lake_dir)
create_table(mtcars, "cars")

begin_transaction()
#> Transaction started.
rows_delete(
  get_ducklake_table("cars"),
  data.frame(gear = 3),
  by = "gear"
)

# Something went wrong, rollback
rollback_transaction()
#> Transaction rolled back.

detach_ducklake("rollback_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
