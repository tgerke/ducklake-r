# Set metadata for the most recent snapshot

Updates the author, commit message, and/or extra info for the most
recent snapshot in a DuckLake catalog.

## Usage

``` r
set_snapshot_metadata(
  ducklake_name,
  author = NULL,
  commit_message = NULL,
  commit_extra_info = NULL,
  conn = NULL
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

## Value

Invisibly returns TRUE on success

## Details

This function updates the metadata columns in the
`ducklake_snapshot_changes` table for the most recent snapshot. Call
this after
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
to add audit information to your commits.

## Examples

``` r
if (FALSE) { # \dontrun{
begin_transaction()
# ... make changes ...
commit_transaction()

# Add metadata to the snapshot
set_snapshot_metadata(
  ducklake_name = "my_ducklake",
  author = "Data Team",
  commit_message = "Updated station names for clarity"
)
} # }
```
