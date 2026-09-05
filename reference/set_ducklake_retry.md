# Configure how DuckLake retries conflicting transactions

With several writers on one lake (a PostgreSQL or SQLite catalog, or a
Quack server), two transactions can try to commit the next snapshot at
the same time. DuckLake settles the race itself: the transaction that
lost checks whether its changes conflict with the one that won, and if
they do not it is retried against the new snapshot. Only real conflicts
reach you as an error: two transactions deleting from the same data
file, an insert into a table the other transaction dropped or altered,
or two creates of the same name. This function sets the DuckDB settings
that control the retries. DuckLake's defaults are 10 attempts, 100 ms
apart, with the wait growing by a factor of 1.5 each time.

## Usage

``` r
set_ducklake_retry(max_retries = NULL, wait_ms = NULL, backoff = NULL)
```

## Arguments

- max_retries:

  Maximum number of retry attempts (a non-negative integer).

- wait_ms:

  Milliseconds to wait before the first retry.

- backoff:

  Factor by which the wait grows after each retry (1 keeps it constant).

## Value

Invisibly, a data frame with the current `name` and `value` of the three
settings.

## Details

The settings belong to the connection, not the lake, so set them in each
session that writes concurrently. Called with no arguments, the function
only reports the current values.

## See also

Other transactions:
[`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md),
[`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md),
[`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md),
[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md),
[`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)

## Examples

``` r
# Be more patient with a busy shared catalog
set_ducklake_retry(max_retries = 20, wait_ms = 250, backoff = 2)
#> DuckLake retries a conflicting transaction up to 20 times, starting 250 ms
#> apart with backoff 2.0.

# Read the current values
set_ducklake_retry()
#> DuckLake retries a conflicting transaction up to 20 times, starting 250 ms
#> apart with backoff 2.0.
```
