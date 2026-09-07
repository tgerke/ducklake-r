# Set metadata for the most recent snapshot

Sets the author, commit message, and/or extra info for the most recent
snapshot in a DuckLake catalog by updating the
`ducklake_snapshot_changes` metadata table directly.

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

This function retroactively updates metadata on the most recent
snapshot. To set metadata at commit time, use the `author`,
`commit_message`, and `commit_extra_info` arguments in
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
or
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
instead.

## Examples

``` r
if (FALSE) { # \dontrun{
begin_transaction()
# ... make changes ...
commit_transaction()

# Add metadata to the snapshot after the fact
set_snapshot_metadata(
  ducklake_name = "my_ducklake",
  author = "Data Team",
  commit_message = "Updated station names for clarity"
)
} # }
```
