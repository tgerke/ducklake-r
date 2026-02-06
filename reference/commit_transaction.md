# Commit a transaction

Commits the current transaction, making all changes permanent.
Optionally adds metadata (author, commit message, and extra info) to the
snapshot.

## Usage

``` r
commit_transaction(
  conn = NULL,
  author = NULL,
  commit_message = NULL,
  commit_extra_info = NULL
)
```

## Arguments

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

- author:

  Optional author name to associate with the snapshot

- commit_message:

  Optional commit message describing the changes

- commit_extra_info:

  Optional extra information about the commit

## Value

Invisibly returns TRUE on success

## Details

This function commits all changes made since
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)
was called, making them permanent in the database. DuckLake
automatically tracks changes in the `ducklake_snapshot_changes` metadata
table.

If `author`, `commit_message`, or `commit_extra_info` are provided, they
will be automatically added to the snapshot metadata after committing.

## Examples

``` r
if (FALSE) { # \dontrun{
# Basic commit
begin_transaction()
# ... make changes ...
commit_transaction()

# Commit with metadata
begin_transaction()
create_table(mtcars, "cars")
commit_transaction(
  author = "John Doe",
  commit_message = "Add cars dataset"
)
} # }
```
