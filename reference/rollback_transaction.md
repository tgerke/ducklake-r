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

## Examples

``` r
if (FALSE) { # \dontrun{
begin_transaction()
# ... make changes ...
# Something went wrong, rollback
rollback_transaction()
} # }
```
