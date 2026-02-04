# Commit a transaction

Commits the current transaction, making all changes permanent.

## Usage

``` r
commit_transaction(conn = NULL)
```

## Arguments

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

## Value

Invisibly returns TRUE on success

## Details

This function commits all changes made since
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)
was called, making them permanent in the database. DuckLake
automatically tracks changes in the `ducklake_snapshot_changes` metadata
table.

To add author and commit message metadata to the snapshot, use
[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
after committing.

## Examples

``` r
if (FALSE) { # \dontrun{
begin_transaction()
# ... make changes ...
commit_transaction()

# Optionally add metadata to the snapshot
set_snapshot_metadata(
  ducklake_name = "my_ducklake",
  author = "John Doe",
  commit_message = "Updated customer records"
)
} # }
```
