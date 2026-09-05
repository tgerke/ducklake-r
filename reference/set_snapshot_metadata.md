# Set metadata for the most recent snapshot

Fills in the author, commit message, and/or extra info of the most
recent snapshot in a DuckLake catalog after it was committed, by
updating the `ducklake_snapshot_changes` metadata table directly.

## Usage

``` r
set_snapshot_metadata(
  ducklake_name,
  author = NULL,
  commit_message = NULL,
  commit_extra_info = NULL,
  conn = NULL,
  overwrite = FALSE
)
```

## Arguments

- ducklake_name:

  The name of the DuckLake catalog

- author:

  Optional author name to associate with the snapshot

- commit_message:

  Optional commit message describing the changes

- commit_extra_info:

  Optional extra information about the commit

- conn:

  Optional DuckDB connection object. If not provided, uses the default
  ducklake connection.

- overwrite:

  Replace values the snapshot already carries (default `FALSE`). By
  default only empty fields are filled in, and the call stops when a
  supplied field already has a value.

## Value

Invisibly returns TRUE on success

## Details

Metadata belongs on the commit: pass `author`, `commit_message`, and
`commit_extra_info` to
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
or
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md),
which record them through DuckLake's `set_commit_message()` as part of
the transaction itself. This function is the escape hatch for a snapshot
that was committed without them, such as one made interactively or by a
client that could not set them.

It writes to the catalog's metadata table outside DuckLake's transaction
and conflict model, and an overwrite leaves no trace of the previous
value. That is why it fills blanks only unless `overwrite = TRUE`. Where
the snapshot history is the audit trail (GxP, 21 CFR Part 11), set
metadata at commit time and leave `overwrite` alone.

## See also

Other transactions:
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md),
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md),
[`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md),
[`set_ducklake_retry()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_retry.md),
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)

## Examples

``` r
lake_dir <- tempfile("meta_lake_")
dir.create(lake_dir)
attach_ducklake("meta_lake", lake_path = lake_dir)

begin_transaction()
#> Transaction started.
create_table(mtcars, "cars")
commit_transaction()
#> Transaction committed.

# The snapshot has no author or message yet: fill them in
set_snapshot_metadata(
  ducklake_name = "meta_lake",
  author = "Data Team",
  commit_message = "Added the cars dataset"
)
#> Snapshot metadata updated.

# A second call refuses to replace them unless told to
try(set_snapshot_metadata("meta_lake", commit_message = "Reworded"))
#> Error in set_snapshot_metadata("meta_lake", commit_message = "Reworded") : 
#>   The latest snapshot already has commit_message set.
#> ℹ Metadata belongs on the commit: record it with `with_transaction()` or
#>   `commit_transaction()`.
#> ℹ Pass `overwrite = TRUE` to replace it; the previous value is not kept.
set_snapshot_metadata("meta_lake", commit_message = "Reworded", overwrite = TRUE)
#> Snapshot metadata updated.

detach_ducklake("meta_lake", shutdown = TRUE)
unlink(lake_dir, recursive = TRUE)
```
