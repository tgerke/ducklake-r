# ducklake: Interact with 'DuckLake' from R

A 'tidyverse'-friendly interface to 'DuckLake'
<https://ducklake.select/>, the 'DuckDB' lakehouse format. Attach
versioned data lakes from R and work with them using familiar 'dplyr'
verbs, with support for ACID transactions, time travel queries, snapshot
audit trails, data inlining, encrypted storage, multiple catalog
backends ('DuckDB', 'PostgreSQL', 'SQLite', 'MySQL'), and remote access
over the 'Quack' protocol from 'DuckDB'.

## Package options

- `ducklake.author`:

  The author recorded on the snapshots this session commits when a call
  does not name one:
  [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md),
  [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md),
  [`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md),
  and the creation snapshot
  [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  labels. An `author` argument wins over the option.
  [`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
  does not read it, because labeling a snapshot after the fact is a
  deliberate edit, and a write made outside a transaction
  ([`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
  on its own) records no author either way. Unset by default.

- `ducklake.verbose`:

  When `FALSE`, the confirmations the package emits after each operation
  ("Committed snapshot 3: ...", "Added column ...") are suppressed,
  which keeps pipeline logs quiet. Warnings, errors, and notices about
  extension downloads are unaffected. The messages carry the condition
  class `ducklake_message`. Default `TRUE`.

## See also

Useful links:

- <https://tgerke.github.io/ducklake-r/>

- <https://github.com/tgerke/ducklake-r>

- Report bugs at <https://github.com/tgerke/ducklake-r/issues>

## Author

**Maintainer**: Travis Gerke <travisgerke@gmail.com>

Authors:

- Travis Gerke <travisgerke@gmail.com>

Other contributors:

- Stefan Linner \[contributor\]

- Javier Orraca-Deatcu \[contributor\]
