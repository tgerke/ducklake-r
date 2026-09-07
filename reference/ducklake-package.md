# ducklake: Interact with 'DuckLake' from R

A 'tidyverse'-friendly interface to 'DuckLake'
<https://ducklake.select/>, the 'DuckDB' lakehouse format. Attach
versioned data lakes from R and work with them using familiar 'dplyr'
verbs, with support for ACID transactions, time travel queries, snapshot
audit trails, data inlining, encrypted storage, multiple catalog
backends ('DuckDB', 'PostgreSQL', 'SQLite', 'MySQL'), and remote access
over the 'Quack' protocol from 'DuckDB'.

## Package options

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
