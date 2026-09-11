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

  Author to record on the snapshot. Defaults to the `ducklake.author`
  option when it is set (see
  [`?ducklake`](https://tgerke.github.io/ducklake-r/reference/ducklake-package.md)),
  otherwise none.

- commit_message:

  Optional commit message describing the changes

- commit_extra_info:

  Optional extra information about the commit

## Value

Invisibly returns TRUE on success

## Details

This function commits all changes made since
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)
was called, making them permanent in the database.

If `author`, `commit_message`, or `commit_extra_info` are provided, they
will be set using `CALL ducklake.set_commit_message()` within the
transaction before the `COMMIT` statement, as required by the DuckLake
v1.0 specification. An author set once for the session with
`options(ducklake.author = "...")` is recorded on every commit that does
not name one; the `author` argument wins when both are given.

The commit is confirmed with one message naming the snapshot it created,
with the author and commit message when they were given. The id comes
from DuckLake's `last_committed_snapshot()`, which tracks this
connection's own commits, so it is right even when other sessions commit
to the same lake at the same time. A transaction that changed nothing
creates no snapshot, and the message says so, naming the snapshot the
lake stands at. `options(ducklake.verbose = FALSE)` silences these
confirmations.

## See also

Other transactions:
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md),
[`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md),
[`set_ducklake_retry()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_retry.md),
[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md),
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)

## Examples

``` r
lake_dir <- tempfile("commit_lake_")
dir.create(lake_dir)
attach_ducklake("commit_lake", lake_path = lake_dir)

# Basic commit
begin_transaction()
create_table(iris, "flowers")
#> Converted factor column Species to character (DuckLake does not support ENUM
#> columns).
commit_transaction()
#> Committed snapshot 1.

# Commit with metadata
begin_transaction()
create_table(mtcars, "cars")
commit_transaction(
  author = "John Doe",
  commit_message = "Add cars dataset"
)
#> Committed snapshot 2 (John Doe): Add cars dataset

detach_ducklake("commit_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
