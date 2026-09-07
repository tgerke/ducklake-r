# Changelog

## ducklake (development version)

- The ducklake DuckDB extension no longer needs an install step. The
  code never required one:
  [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  has always installed it on first use, after a message naming the
  directory. The README and the articles now say so, and
  [`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)
  is documented as the way to download the extension ahead of time
  (container images, CI, machines that are offline when the lake is
  attached). A first-use install now also clears the answer
  [`ducklake_extension_available()`](https://tgerke.github.io/ducklake-r/reference/ducklake_extension_available.md)
  cached, so it reports `TRUE` afterwards. The hint that follows an
  install into a temporary directory did not show on macOS for a
  first-use install, because the directory does not exist until
  `INSTALL` creates it and the check only recognized existing paths; it
  shows now. No load-time check was added: the package neither downloads
  nor starts DuckDB when it is loaded.

- The minimum duckdb version is now 1.5.5, the release that settled
  where the R package keeps downloaded extensions: `~/.duckdb` when it
  exists (duckdb offers to create it the first time it connects in an
  interactive session), `DUCKDB_R_HOME` or the `duckdb.home` option when
  set, otherwise a per-session temporary directory
  ([`?duckdb::duckdb_storage`](https://r.duckdb.org/reference/duckdb_storage.html)).
  [`ducklake_extension_available()`](https://tgerke.github.io/ducklake-r/reference/ducklake_extension_available.md)
  now opens its probe on the directory duckdb would resolve for a new
  connection, so it finds an existing `~/.duckdb` without triggering
  that offer, and the hint printed after an install into a temporary
  directory describes these options.

- [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
  and
  [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
  confirm a commit with one line naming the snapshot it created, with
  the author and commit message when they were given: “Committed
  snapshot 3 (Data Engineer): Add the Motor Trend car data”. A
  transaction that changed nothing says so, and
  [`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)
  is silent. The two lines they replace (“Transaction started.”,
  “Transaction committed.”) said nothing about the snapshot; the new one
  gives the version number that time travel uses.

- The Getting Started article
  ([`vignette("ducklake")`](https://tgerke.github.io/ducklake-r/articles/ducklake.md))
  is rewritten as one short session: install, attach a lake, add a
  table, read it back, change it, see its history, and detach, with the
  reasons to wrap changes in
  [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
  explained along the way. Its recipes move to two new articles.
  “Loading Data”
  ([`vignette("loading-data")`](https://tgerke.github.io/ducklake-r/articles/loading-data.md))
  collects the ways data gets into a lake, from data frames and files to
  registering Parquet in place and migrating from DuckDB or Iceberg.
  “Views, Comments, and Labels”
  ([`vignette("views-comments-labels")`](https://tgerke.github.io/ducklake-r/articles/views-comments-labels.md))
  covers the query logic and documentation that live in the catalog, and
  its labels example uses
  [`labelled::set_variable_labels()`](https://larmarange.github.io/labelled/reference/var_label.html),
  so labelled is now a suggested package.

## ducklake 0.7.0

- New article “Choosing a Deployment”
  ([`vignette("deployment")`](https://tgerke.github.io/ducklake-r/articles/deployment.md)):
  what a lake looks like on disk, which catalog backend fits one person,
  a team on a shared drive, or many users on object storage, where data
  can go and the Windows limits, how reader and writer roles map onto
  catalog grants and storage permissions, the day-one settings
  (extension persistence, schemas, retention policy, commit messages),
  and upgrading.

- New
  [`get_ducklake_info()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_info.md)
  describes an attached lake in one row: backend, catalog, data path,
  DuckLake format version, extension version, encryption, and current
  snapshot.

- New
  [`set_column_not_null()`](https://tgerke.github.io/ducklake-r/reference/set_column_not_null.md)
  sets or drops a `NOT NULL` constraint, the one constraint DuckLake
  supports.

- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  creates a local `lake_path` (and the directory of a local catalog
  file) when it does not exist and creating the lake is allowed, instead
  of failing with DuckDB’s “Cannot open file” error.

- [`?set_ducklake_option`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md)
  lists every option DuckLake 1.0 persists, with its default and what it
  controls.

- New cookbook recipes migrate an existing DuckDB database into a lake
  with `COPY FROM DATABASE` and exchange tables with an Iceberg catalog;
  the time-travel vignette documents the `rowid` and `snapshot_id`
  hidden columns.

- [`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md)
  copies the catalog with DuckDB’s `COPY FROM DATABASE` while the lake
  stays attached, a consistent snapshot taken inside one transaction,
  instead of shutting the connection down to release file locks and
  copying the file. Nothing is detached any more: other attached lakes,
  in-memory secrets, and a connection registered with
  [`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)
  (whose locks the old approach could not release, leaving a 0-byte
  catalog) are left as they are. A SQLite catalog is copied into a
  SQLite file. Restoring a backup is documented with `create = FALSE`,
  so a mistyped path is an error rather than a new lake.

- New package option `ducklake.verbose`: set it to `FALSE` to silence
  the confirmations the package emits after each operation (“Transaction
  committed.”, “Added column …”). Warnings, errors, and notices about
  extension downloads stay on. The messages carry the condition class
  `ducklake_message`.

- Schema-qualified table names work end to end. Every function that
  takes a table name accepts `"schema.table"`:
  [`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md)
  hands dbplyr a proper table path for it (the duckdb driver’s
  [`tbl()`](https://dplyr.tidyverse.org/reference/tbl.html) turned a
  dotted name into raw SQL that `rows_*()` could not write to), and the
  metadata readers
  ([`get_table_comments()`](https://tgerke.github.io/ducklake-r/reference/get_table_comments.md),
  [`get_table_partitions()`](https://tgerke.github.io/ducklake-r/reference/get_table_partitions.md),
  [`get_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/get_table_sorting.md),
  [`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md),
  [`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md),
  [`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md),
  [`list_ducklake_files()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_files.md))
  resolve the schema instead of matching the bare name. Functions with a
  `schema_name` argument take the schema from either place. The readers
  gain a `schema_name` column. New
  [`create_schema()`](https://tgerke.github.io/ducklake-r/reference/create_schema.md)
  and
  [`drop_schema()`](https://tgerke.github.io/ducklake-r/reference/drop_schema.md)
  manage schemas, the natural home for medallion layers; the README
  example now keeps bronze, silver, and gold in schemas of their own.

- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  gains `create` (`FALSE` opens an existing lake and errors on a wrong
  path or name instead of creating a new, empty lake) and
  `metadata_schema` (several lakes in one PostgreSQL database, each in
  its own schema).

- New
  [`set_ducklake_retry()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_retry.md)
  sets how DuckLake retries a transaction that races with another
  writer, and the transactions vignette explains which concurrent
  changes conflict and which are retried.

- [`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
  and
  [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
  now run dplyr pipelines inside DuckDB. A lazy table on the package’s
  connection is written with `CREATE TABLE ... AS`; a replacement is
  materialized in DuckDB’s temporary storage first (the query may read
  the table it replaces) and the table is rebuilt from it. Rows no
  longer pass through R, so silver and gold layers derived from large
  bronze tables cost DuckDB memory, not R memory. Column comments follow
  the data: each output column keeps the comment of the same-named
  column in the tables the query reads, so variable labels survive a
  pipeline as they did through the old collect path. Data frames, and
  lazy tables on other connections, load as before.

- [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
  and
  [`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)
  now carry a table’s metadata over to the rewritten table: the table
  comment, column comments (and so variable labels), partition keys,
  sort order, and table-scoped options. Both go through DROP + CREATE,
  which gives the table a new id, and DuckLake keeps all of these
  against the id, so they were silently lost before. Partition and sort
  keys are set before the rows are written, so the rewrite itself lands
  partitioned and sorted. Table-scoped options are re-set right after
  the rewrite commits (DuckLake cannot set options on a table created in
  the open transaction); inside a transaction you opened yourself they
  cannot be re-set, and a warning lists the calls to make after
  committing. New
  [`get_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/get_table_sorting.md)
  reads a table’s sort keys from the catalog, the counterpart of
  [`get_table_partitions()`](https://tgerke.github.io/ducklake-r/reference/get_table_partitions.md).

- The
  [`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md)
  recipe for re-partitioning existing data (set the keys, then rewrite
  with
  [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md))
  now works; before, the rewrite dropped the keys it was meant to apply.

- [`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md)
  no longer claims to expire old snapshots and reclaim their files on
  its own. A checkpoint does so only when the lake carries a retention
  policy (`expire_older_than` and `delete_older_than`, set with
  [`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md));
  without one it flushes and compacts but keeps every snapshot and every
  file. The storage and data-inlining vignettes and the cookbook now
  show the policy.

- [`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
  fills in only empty fields by default and stops when a supplied field
  already has a value; pass `overwrite = TRUE` to replace one. It writes
  to the catalog outside DuckLake’s transaction model, and an overwrite
  leaves no trace of the previous value, so the audited path is metadata
  set at commit time with
  [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
  or
  [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md).
  The documentation now says so plainly.

- The minimum duckdb version is now 1.5.2, the release that ships
  DuckLake 1.0. The extension built for DuckDB 1.5.1 writes the earlier
  0.4 catalog format, so a lake created there needs a one-time migration
  after the upgrade:
  [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  gains `automatic_migration = TRUE` for that (DuckLake’s
  `AUTOMATIC_MIGRATION` option).

- Where the ducklake extension is installed depends on the duckdb R
  package: from 1.5.2 on it is a per-session temporary directory unless
  `DUCKDB_R_HOME` (or the `duckdb.home` option, or an existing
  `~/.duckdb`) points somewhere durable, so “install once per machine”
  needs that setting.
  [`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)
  and the automatic extension installs now report the directory they
  used and say when it is temporary; the README,
  [`ducklake_extension_available()`](https://tgerke.github.io/ducklake-r/reference/ducklake_extension_available.md),
  and `create_storage_secret(persistent = TRUE)` describe the setting.

- The clinical trial vignette’s silver layer called
  [`admiral::convert_blanks_to_na()`](https:/pharmaverse.github.io/admiral/v1.5.0/cran-release/reference/convert_blanks_to_na.html)
  on a lazy lake table, which leaves the table untouched, so the
  cleaning it described never ran. The conversion now runs inside DuckDB
  through a small dbplyr helper. (The pharmaverse test data already
  stores missing values as `NA`, so the vignette’s output does not
  change; XPT exports do carry blanks.)

- Vignettes follow the package’s own guidance on how to change a table:
  derived columns are declared with
  [`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)
  and filled with
  [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md),
  corrections use
  [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md)
  and
  [`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md),
  and
  [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
  is kept for bulk rewrites.

- [`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md)
  and
  [`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md)
  return the same `tbl_ducklake` class as
  [`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md),
  so [`collect()`](https://dplyr.tidyverse.org/reference/compute.html)
  restores stored column labels on time-travel reads too.

- `list_table_snapshots(table_name)` matches snapshots by parsing the
  change map rather than by a regular expression over its printed form,
  and resolves table ids within the table’s schema.

- Functions that read a named lake’s metadata
  ([`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
  [`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md),
  [`get_table_partitions()`](https://tgerke.github.io/ducklake-r/reference/get_table_partitions.md),
  [`get_table_comments()`](https://tgerke.github.io/ducklake-r/reference/get_table_comments.md),
  [`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md))
  resolve the catalog backend from that lake rather than from the
  current database, so a PostgreSQL or MySQL lake that is attached but
  not current is qualified correctly.

- `ducklake_exec(.quiet = FALSE)` emits its SQL trace as messages
  instead of printing it, and no longer prints the lazy table itself,
  which ran a preview query.
  [`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)
  prints through cli.

- The documentation notes that the aws and azure DuckDB extensions are
  not available on Windows for the duckdb R package, so
  `create_storage_secret(provider = "credential_chain")` and Azure
  secrets do not work there. The README now says Quack needs duckdb
  1.5.3 or newer, not 1.5.4.

- [`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)
  now works with logical, Date, and POSIXct defaults. DuckLake accepts
  only plain constants in a DEFAULT clause and rejected the typed
  literals the package rendered (`TRUE`, `DATE '...'`,
  `TIMESTAMP '...'`) as “non-literal”; such values are now passed as
  quoted strings, which DuckLake converts to the column type.

- A failed schema evolution statement no longer leaves the shared
  connection in an aborted transaction. Some DuckLake DDL failures do
  that even in autocommit mode, so that every later statement failed
  with “Current transaction is aborted”; the wrappers now roll back
  before re-raising the error when they did not inherit a transaction.

- The unreferenced and broken `inst/examples/with_transaction_demo.R`
  was removed; the
  [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
  examples cover it.

- New `meta_encryption_key` argument in
  [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  encrypts the DuckDB catalog database file itself with AES-256-GCM
  (DuckLake forwards `META_`-prefixed options to the metadata catalog).
  The key is set when the catalog is created and required on every later
  attach. The catalog is where `encrypted = TRUE` stores its Parquet
  keys, so encrypting it closes that loop; pass
  [`askpass::askpass()`](https://r-lib.r-universe.dev/askpass/reference/askpass.html)
  as the value to be prompted instead of writing the key in code
  ([\#46](https://github.com/tgerke/ducklake-r/issues/46), suggested by
  [@frankpopham](https://github.com/frankpopham)).

- The `"duckdb"` backend now honors `catalog_connection_string` as the
  path for its catalog file, so a single-writer lake can keep the
  catalog on local disk while `lake_path` points at object storage. The
  documentation already promised this argument worked for the duckdb
  backend; now it does
  ([\#44](https://github.com/tgerke/ducklake-r/issues/44)).

- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  stops early when the duckdb backend would create its catalog file on
  object storage, which DuckDB cannot write and which previously
  surfaced as a confusing IO error. The message points to
  `catalog_connection_string`, to `read_only = TRUE` (attaching an
  existing remote catalog stays supported), and to the other backends.
  httpfs is now loaded up front whenever a remote path is involved
  instead of relying on DuckDB’s mid-statement autoload.

- The
  [`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md)
  example and the storage vignette attached an S3 lake with the default
  backend and no catalog path, which would put the catalog file itself
  on S3. Both now show the split layout, and
  [`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md)
  finds a catalog that lives outside `lake_path`.

- `create_storage_secret(provider = "credential_chain")` now loads
  DuckDB’s aws extension itself for `"s3"`, `"gcs"`, and `"r2"` secrets,
  installing it on first use. The provider lives in that extension, and
  DuckDB’s automatic mid-statement install of it could fail, leaving
  `CREATE SECRET` erroring with “Install it first”
  ([\#43](https://github.com/tgerke/ducklake-r/issues/43)).

## ducklake 0.6.0

First CRAN release.

- New
  [`ducklake_extension_available()`](https://tgerke.github.io/ducklake-r/reference/ducklake_extension_available.md)
  reports whether the `ducklake` DuckDB extension is installed and
  loadable. It probes with automatic extension installation switched
  off, so it never downloads anything, and it is what the package’s own
  examples, tests, and vignettes gate on.

- Every example that can run against a temporary lake now does, guarded
  by `@examplesIf ducklake_extension_available()`. Only the cases that
  need outside infrastructure stay unevaluated: Quack servers,
  PostgreSQL and MySQL catalogs, and remote URLs.

- [`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md)
  no longer needs the fs package. It was the one place in the package
  that called a suggested dependency unconditionally, so backups failed
  for anyone without fs installed.

- The package now tells you before it installs a DuckDB extension for
  you.
  [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  and the functions that load `httpfs`, `quack`, or a backend extension
  used to download into the extension cache in your home directory
  without a word.

- Fixed the
  [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
  rollback example, which reused a table name that already existed and
  so failed before reaching the error it meant to demonstrate.

## ducklake 0.5.0

- New
  [`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)
  completes the dplyr `rows_*` family: rows that match on the key
  columns are updated and the rest are inserted, as one atomic
  `MERGE INTO` statement (DuckLake tables have no primary keys, so the
  `ON CONFLICT` upsert other databases use does not apply). One call is
  one snapshot, with the updates and inserts recorded individually in
  the change feed.

- New
  [`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md)
  exposes the full SQL MERGE surface for the cases
  [`rows_upsert()`](https://tgerke.github.io/ducklake-r/reference/rows_upsert.md)
  cannot express: conditional matched and not-matched clauses, deleting
  matched rows, and `delete_missing = TRUE` to drop target rows absent
  from the source (a staging-table sync). DuckLake currently allows one
  update/delete action per MERGE statement, so syncs that need both run
  as MERGE plus DELETE inside a single transaction and snapshot.

- New table documentation family, built for labelled-data workflows:
  [`set_table_comment()`](https://tgerke.github.io/ducklake-r/reference/set_table_comment.md)
  and
  [`set_column_comments()`](https://tgerke.github.io/ducklake-r/reference/set_column_comments.md)
  store descriptions in the lake’s catalog (`COMMENT ON`), and
  [`get_table_comments()`](https://tgerke.github.io/ducklake-r/reference/get_table_comments.md)
  reads them back as a tidy data frame.
  [`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
  gains a `labels` argument (default `TRUE`) that stores haven/labelled
  variable labels as column comments at load time, and
  [`collect()`](https://dplyr.tidyverse.org/reference/compute.html) on a
  lake table reattaches stored comments as `label` attributes – so
  gtsummary, gt, and other label-aware tools work as if the data never
  left R, and every other client of the lake can read the same
  documentation.

- New
  [`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md)
  stores a dplyr pipeline as a SQL view in the lake: shared business
  logic that reads current data and that every client – R, Python, or
  plain SQL – sees identically.
  [`drop_view()`](https://tgerke.github.io/ducklake-r/reference/drop_view.md)
  removes one. SQL macros stay unwrapped on purpose (a macro body is raw
  SQL and unreachable from dplyr pipelines); the cookbook shows the
  [`DBI::dbExecute()`](https://dbi.r-dbi.org/reference/dbExecute.html)
  escape hatch.

- New
  [`list_ducklake_tables()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_tables.md)
  answers “what is in this lake?” with a tidy frame of tables and views.

- New schema evolution family:
  [`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)
  (with optional `default`, which DuckLake applies to existing rows
  too),
  [`drop_table_column()`](https://tgerke.github.io/ducklake-r/reference/drop_table_column.md),
  [`rename_table_column()`](https://tgerke.github.io/ducklake-r/reference/rename_table_column.md),
  [`set_column_type()`](https://tgerke.github.io/ducklake-r/reference/set_column_type.md)
  (widening promotions only, with the add-copy-drop-rename recipe in the
  error when a change would narrow), and
  [`rename_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/rename_ducklake_table.md).
  All are metadata-only `ALTER TABLE` operations: no data files are
  rewritten, and earlier snapshots keep the earlier schema. Until now
  schema changes went through
  [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
  which collects the whole table into R; its documentation now points
  here, and it remains the tool for bulk *data* transformations. Derived
  columns combine the two styles:
  [`add_table_column()`](https://tgerke.github.io/ducklake-r/reference/add_table_column.md)
  then a [`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html)
  pipeline through
  [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
  fills the column with an in-database UPDATE.

- New plotting functions, all requiring the suggested ggplot2 package
  and shown in action in a new “Visualizing Your Lake” vignette:
  [`plot_snapshots()`](https://tgerke.github.io/ducklake-r/reference/plot_snapshots.md)
  draws a table’s snapshot history as a commit-log timeline (snapshots
  in order, with authors, commit messages, and inline markers for long
  idle gaps) or, without a table name, the whole lake as a swimlane with
  one row per table;
  [`plot_table_changes()`](https://tgerke.github.io/ducklake-r/reference/plot_table_changes.md)
  draws the rows each snapshot inserted, updated, and deleted as
  diverging bars; and
  [`plot_table_files()`](https://tgerke.github.io/ducklake-r/reference/plot_table_files.md)
  draws each table’s Parquet file count and size on disk.

- New
  [`get_table_info()`](https://tgerke.github.io/ducklake-r/reference/get_table_info.md)
  returns per-table file statistics (data and delete file counts and
  sizes) from the DuckLake catalog, wrapping DuckLake’s
  `ducklake_table_info()` function.

- New
  [`add_data_files()`](https://tgerke.github.io/ducklake-r/reference/add_data_files.md)
  registers existing Parquet files with a table without copying or
  rewriting them – the migration path for data that is already in
  Parquet. A vector of files is registered atomically in one snapshot,
  and `create = TRUE` can bootstrap a new target table directly from the
  Parquet schema.
  [`list_ducklake_files()`](https://tgerke.github.io/ducklake-r/reference/list_ducklake_files.md)
  shows the files backing a table, optionally as of a past snapshot.

- New sorted-table support:
  [`set_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/set_table_sorting.md)
  and
  [`reset_table_sorting()`](https://tgerke.github.io/ducklake-r/reference/reset_table_sorting.md)
  manage a table’s declared sort order, the complement to partitioning
  for pruning on high-cardinality columns.

- New
  [`set_ducklake_option()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_option.md)
  and
  [`get_ducklake_options()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_options.md)
  expose DuckLake’s full option system (`parquet_compression`,
  `target_file_size`, `sort_on_insert`, `require_commit_message`, …) at
  lake, schema, or table scope.
  [`set_inlining_row_limit()`](https://tgerke.github.io/ducklake-r/reference/set_inlining_row_limit.md)
  now builds on the same internals.

- New
  [`create_storage_secret()`](https://tgerke.github.io/ducklake-r/reference/create_storage_secret.md)
  stores object-storage credentials (S3, GCS, R2, Azure) via DuckDB’s
  secrets manager, so a lake’s `lake_path` can live on cloud storage.
  [`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md)
  now errors clearly for remote data paths instead of failing partway
  through.

- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  gains `snapshot_version` and `snapshot_time` arguments to attach a
  lake pinned to a historical snapshot – a frozen, read-only view for
  reproducing past analyses.

- [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
  now runs its drop and create as one transaction, so a failed create no
  longer leaves the table dropped. When the caller has already opened a
  transaction,
  [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
  defers to it as before.

- [`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
  now validates `ducklake_name` like the rest of the package, and
  resolves the catalog backend from that name instead of the current
  database.

- [`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
  no longer leaves a temporary view registered on the shared connection
  when the CREATE statement fails.

- `replace_table(.quiet = FALSE)` reports progress via messages (cli)
  rather than printing to the console, so it can be suppressed and
  captured like the rest of the package’s output.

- The package now declares R (\>= 4.1) explicitly (the tests and
  examples use the base pipe), and is prepared for CRAN:
  extension-dependent tests skip on CRAN, and vignettes evaluate only
  where the ducklake DuckDB extension can be loaded.

- New targeted maintenance wrappers complement
  [`checkpoint_ducklake()`](https://tgerke.github.io/ducklake-r/reference/checkpoint_ducklake.md):
  [`expire_snapshots()`](https://tgerke.github.io/ducklake-r/reference/expire_snapshots.md)
  (with `older_than`, `versions`, and `dry_run`),
  [`merge_adjacent_files()`](https://tgerke.github.io/ducklake-r/reference/merge_adjacent_files.md),
  [`cleanup_old_files()`](https://tgerke.github.io/ducklake-r/reference/cleanup_old_files.md),
  [`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md),
  and
  [`rewrite_data_files()`](https://tgerke.github.io/ducklake-r/reference/rewrite_data_files.md)
  ([\#16](https://github.com/tgerke/ducklake-r/issues/16), suggested by
  [@stefanlinner](https://github.com/stefanlinner)).

- New partitioning support:
  [`set_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/set_table_partitioning.md)
  and
  [`reset_table_partitioning()`](https://tgerke.github.io/ducklake-r/reference/reset_table_partitioning.md)
  manage a table’s partition keys (identity,
  `year`/`month`/`day`/`hour`, and `bucket` transforms), and
  [`get_table_partitions()`](https://tgerke.github.io/ducklake-r/reference/get_table_partitions.md)
  lists the keys from the metadata catalog
  ([\#16](https://github.com/tgerke/ducklake-r/issues/16), suggested by
  [@stefanlinner](https://github.com/stefanlinner)).

- New
  [`get_table_changes()`](https://tgerke.github.io/ducklake-r/reference/get_table_changes.md)
  exposes DuckLake’s data change feed: the exact inserts, deletes, and
  update pre/post images between two snapshots, as a lazy table that
  composes with dplyr verbs
  ([\#16](https://github.com/tgerke/ducklake-r/issues/16), suggested by
  [@stefanlinner](https://github.com/stefanlinner)).

- Timestamps passed to the new functions as POSIXct are converted to UTC
  before interpolation, matching how DuckLake records snapshot times.

- [`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md)
  and
  [`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)
  now also convert POSIXct timestamps to UTC. Previously they rendered
  local time, which DuckLake reads as UTC, silently shifting the queried
  instant by the UTC offset – a bare
  [`Sys.time()`](https://rdrr.io/r/base/Sys.time.html) looked hours in
  the past (or future) unless the session’s timezone was UTC. Timestamps
  taken from `list_table_snapshots()$snapshot_time` are unaffected.

- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  now collapses duplicate slashes in `lake_path` (remote URIs are
  untouched). DuckLake compares file paths as exact strings, so a
  doubled slash – which R’s
  [`tempdir()`](https://rdrr.io/r/base/tempfile.html) produces on macOS
  – made
  [`delete_orphaned_files()`](https://tgerke.github.io/ducklake-r/reference/delete_orphaned_files.md)
  treat every live data file as orphaned.

- The dplyr-to-DuckLake translation behind
  [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
  and
  [`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)
  is now built from dbplyr’s structured query objects
  ([`dbplyr::sql_build()`](https://dbplyr.tidyverse.org/reference/sql_build.html))
  instead of pattern-matching rendered SQL text. Classification no
  longer depends on what the SQL happens to look like, which fixes
  several latent bugs:

  - A filtered read from *another* table
    (`get_ducklake_table("staging") |> filter(...) |> ducklake_exec("target")`)
    was translated into a `DELETE` on the target table; it now appends
    the matching rows, as intended.
  - Filter values containing SQL keywords (e.g.
    `filter(note != "WHERE is it")`) were refused as “too complex”; they
    now translate fine.
  - A [`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html)
    that adds a new column is refused upfront with a pointer to
    [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md),
    instead of failing with a database binder error.

- `INSERT` translations now list columns explicitly, so appends from
  another table match columns by name rather than by position, and
  joined or unioned sources can be appended in one step.

- Pipelines that compile to a subquery over the target table (grouped
  filters, filtering on a just-mutated column), and clauses with no
  in-place equivalent
  ([`arrange()`](https://dplyr.tidyverse.org/reference/arrange.html),
  [`head()`](https://rdrr.io/r/utils/head.html),
  [`distinct()`](https://dplyr.tidyverse.org/reference/distinct.html)),
  are detected structurally and refused with a clear message rather than
  mistranslated.

- dbplyr (\>= 2.5.0) is now required; both dbplyr 2.5.x and the
  select-list format introduced in dbplyr 2.6.0 are supported.

## ducklake 0.4.0

This release focuses on production hardiness: self-contained connection
management, working detach/restore, SQL identifier safety, Quack remote
access, and a documentation overhaul.

### Quack remote protocol support

Added support for Quack, DuckDB’s client-server protocol, which became a
core extension in DuckDB 1.5.3
([\#20](https://github.com/tgerke/ducklake-r/issues/20),
[@JavOrraca](https://github.com/JavOrraca)). A DuckLake served by one
DuckDB instance can now be queried and modified by other R sessions over
the network. For concurrent access this is a lighter-weight option than
a PostgreSQL or SQLite catalog, since the whole setup stays inside
DuckDB and DuckLake.

- [`attach_quack()`](https://tgerke.github.io/ducklake-r/reference/attach_quack.md)
  connects to a remote Quack server and attaches it as a catalog in the
  current session.
- [`detach_quack()`](https://tgerke.github.io/ducklake-r/reference/detach_quack.md)
  disconnects from a remote Quack server.
- [`install_quack()`](https://tgerke.github.io/ducklake-r/reference/install_quack.md)
  installs the Quack DuckDB extension.
- [`quack_query()`](https://tgerke.github.io/ducklake-r/reference/quack_query.md)
  runs a one-off query against a remote Quack server and returns a
  data.frame.
- [`quack_serve()`](https://tgerke.github.io/ducklake-r/reference/quack_serve.md)
  serves the current session, including an attached DuckLake, to other
  clients over Quack.
- [`quack_stop()`](https://tgerke.github.io/ducklake-r/reference/quack_stop.md)
  stops a running Quack server.

### Production hardening

#### New Features

- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  gains an `encrypted` argument: pass `encrypted = TRUE` to have
  DuckLake encrypt the Parquet files it writes
  ([\#18](https://github.com/tgerke/ducklake-r/issues/18)). Note that
  the encryption keys are stored in the catalog database, so protect the
  catalog. The httpfs extension is loaded automatically for encrypted
  lakes, since on some platforms (notably Windows) DuckDB’s built-in
  crypto module is read-only.
- [`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)
  now works. It previously generated a `RESTORE TABLE` statement that
  does not exist in DuckLake and failed on every call. It now recreates
  the table from a time-travel read inside a transaction, recording the
  restore as a new snapshot so history is preserved. It also gains
  `author` and `commit_message` arguments so the restore snapshot
  carries full audit-trail metadata.
- [`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md)
  gains a `ducklake_name` argument and tracks each attached lake
  separately, so sessions with several lakes on different catalog
  backends resolve backend-specific behaviour correctly.

#### Bug Fixes

- [`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md)
  now actually detaches. Previously the `DETACH` ran while the lake was
  still the session’s current database, which DuckDB refuses, and the
  error was silently swallowed – the lake stayed attached. The session
  now switches back to the connection’s own catalog first. Relatedly,
  restoring a backup to a new location requires
  `override_data_path = TRUE` (as documented); the storage vignette
  example has been corrected.
- Table names, lake names, and file paths are now quoted or validated
  before being interpolated into SQL
  ([`DBI::dbQuoteIdentifier()`](https://dbi.r-dbi.org/reference/dbQuoteIdentifier.html)
  and friends), so names with spaces or quotes no longer produce
  malformed statements.
- [`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
  [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
  and
  [`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md)
  now also dispatch as S3 methods on tables returned by
  [`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md).
  Previously, if dplyr was loaded *after* ducklake, dplyr’s generics
  masked ducklake’s wrappers and calls failed with `conflict = "error"`
  complaints; load order no longer matters.
- [`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
  [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
  and
  [`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md)
  now work inside
  [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md),
  so several row operations can be grouped into a single snapshot with
  an author and commit message. Previously, passing a local data frame
  made dbplyr copy it to a temporary table inside its own transaction,
  which DuckDB rejects when one is already open. Local data frames are
  now sent as inline queries
  ([`dbplyr::copy_inline()`](https://dbplyr.tidyverse.org/reference/copy_inline.html)),
  which is also faster for the small changesets these functions are
  designed for.
- [`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md)
  backs up every schema directory, not just `main`.
- [`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
  now converts factor columns to character (with a message) instead of
  failing with “unsupported type ENUM” – DuckLake does not support
  DuckDB’s ENUM type, which is what factors become.
- The internal dplyr-to-SQL translation in
  [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
  no longer uses [`sink()`](https://rdrr.io/r/base/sink.html) (which
  could leak diverted output on error), and now refuses queries with
  subqueries or multiple `WHERE` clauses instead of generating incorrect
  SQL.
- [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
  no longer executes its statement twice. The internal translation step
  also executed the SQL before
  [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
  ran it again, so every call created two snapshots and non-idempotent
  updates (e.g. `v = v + 1`) were applied twice.
- [`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md)
  is now a true preview: it previously *executed* the translated
  statement against the lake while displaying it.
- [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md)
  now translates any
  [`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html) into
  an UPDATE, not just those that compile to `CASE WHEN`. Previously a
  simple transformation like `mutate(v = round(v, 1))` fell through to
  an INSERT of the table’s own rows, silently duplicating the table.
  Plain self-reads with nothing to translate are now refused for the
  same reason, and UPDATE assignments containing commas inside function
  calls are parsed correctly.
- `list_table_snapshots(table_name)` no longer misses snapshots created
  by
  [`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md),
  [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md),
  and
  [`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md).
  DuckLake records row-level changes against the table’s numeric id
  rather than its name; the filter now resolves and matches those ids,
  so the per-table audit trail is complete. Filtered listings also
  number their rows from 1 instead of leaking the row positions of the
  unfiltered result.

### Connection management is now self-contained

ducklake now creates and manages its own DuckDB connection instead of
reaching into duckplyr’s unexported internals. This removes the
package’s last `:::` calls and the duckplyr dependency entirely.

#### Breaking Changes

- duckplyr is no longer a dependency. If you relied on ducklake sharing
  duckplyr’s default connection, register a connection explicitly with
  the new
  [`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md).

#### New Features

- [`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)
  (returning by popular demand, now safer): point ducklake at any DuckDB
  connection you manage — for example one shared with other DBI tools.
  Connections you supply are never closed by ducklake; only its own
  automatically created connection is shut down by
  `detach_ducklake(shutdown = TRUE)` and at session exit.

## ducklake 0.3.0

### DuckLake v1.0 Specification Alignment

This release aligns the package with the [DuckLake v1.0 stable
specification](https://ducklake.select/docs/stable/specification/introduction),
which requires DuckDB v1.5.2+ (compatible with duckdb R package \>=
1.5.1).

#### Breaking Changes

- DuckDB version requirement bumped from 1.3.0 to **1.5.1** (duckdb R
  package) / **1.5.2** (DuckDB engine/CLI) to match DuckLake v1.0.
  [`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)
  now enforces this at the engine level.

- [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
  and
  [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md)
  now use the official `CALL ducklake.set_commit_message()` API to set
  commit metadata **within** the transaction before `COMMIT`, consistent
  with the v1.0 specification.
  [`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
  retroactively updates the `ducklake_snapshot_changes` metadata table
  directly.

## ducklake 0.2.0

### Multi-Backend Catalog Support

DuckLake now supports PostgreSQL, SQLite, and MySQL as catalog backends
in addition to DuckDB
([\#15](https://github.com/tgerke/ducklake-r/issues/15),
[@stefanlinner](https://github.com/stefanlinner)). This aligns with the
[DuckLake 1.0
specification](https://ducklake.select/docs/stable/specification/introduction)
and enables concurrent multi-client access when using PostgreSQL or
SQLite.

#### New Features

- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  gains `backend`, `catalog_connection_string`, `read_only`, and
  `override_data_path` parameters for multi-backend support.
- [`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md)
  gains a `backend` parameter to pre-install backend extensions (e.g.,
  `install_ducklake(backend = "postgres")`).
- New
  [`get_ducklake_backend()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_backend.md)
  returns the active catalog backend type.
- [`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md)
  gains a `shutdown` parameter. By default it now performs a soft detach
  (SQL `DETACH` + `USE memory;`) instead of shutting down the
  connection, allowing backend switching within a session.
- [`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md)
  is now backend-aware: file-based backends (DuckDB, SQLite) get
  catalog + data copied; PostgreSQL/MySQL get data only with guidance to
  use `pg_dump`/`mysqldump`. Also fixes a pre-existing bug where catalog
  backups were silently 0 bytes due to DuckDB holding file locks during
  [`file.copy()`](https://rdrr.io/r/base/files.html).

#### Breaking Changes

- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)
  now **requires** `lake_path` (previously optional).
- [`set_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/set_ducklake_connection.md)
  has been removed. The package now exclusively uses duckplyr’s
  singleton DuckDB connection.
- [`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md)
  no longer shuts down the DuckDB connection by default. Pass
  `shutdown = TRUE` for the previous behaviour.

#### Internal

- Schema qualifier logic updated throughout
  ([`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md),
  `time_travel.R`, `transactions.R`) to handle PostgreSQL/MySQL backends
  that don’t use the `.main.` schema prefix.
- New internal helpers:
  [`build_attach_sql()`](https://tgerke.github.io/ducklake-r/reference/build_attach_sql.md),
  [`ensure_extensions()`](https://tgerke.github.io/ducklake-r/reference/ensure_extensions.md),
  `shutdown_and_reset_singleton()`.

------------------------------------------------------------------------

## ducklake 0.1.0

Initial release of ducklake, an R package for versioned data lake
infrastructure built on DuckDB and DuckLake.

### Features

#### Core Table Operations

- [`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md) -
  Create new tables in the data lake
- [`get_ducklake_table()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table.md) -
  Retrieve tables as tibbles
- [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md) -
  Replace entire table contents with versioning

#### Row-Level Operations

- [`rows_insert()`](https://tgerke.github.io/ducklake-r/reference/rows_insert.md) -
  Insert new rows with automatic versioning
- [`rows_update()`](https://tgerke.github.io/ducklake-r/reference/rows_update.md) -
  Update existing rows with audit trail
- [`rows_delete()`](https://tgerke.github.io/ducklake-r/reference/rows_delete.md) -
  Delete rows while maintaining history

#### ACID Transactions

- [`with_transaction()`](https://tgerke.github.io/ducklake-r/reference/with_transaction.md) -
  Execute code blocks within transactions
- [`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md),
  [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md),
  [`rollback_transaction()`](https://tgerke.github.io/ducklake-r/reference/rollback_transaction.md) -
  Manual transaction control
- Full ACID compliance for data integrity

#### Time Travel

- [`get_ducklake_table_asof()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_asof.md) -
  Query table state at specific timestamps
- [`get_ducklake_table_version()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_table_version.md) -
  Retrieve specific table versions
- [`list_table_snapshots()`](https://tgerke.github.io/ducklake-r/reference/list_table_snapshots.md) -
  View complete version history
- [`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md) -
  Roll back to previous versions

#### Metadata and Audit Trail

- [`get_metadata_table()`](https://tgerke.github.io/ducklake-r/reference/get_metadata_table.md) -
  Access comprehensive metadata
- [`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md) -
  Add author, commit messages, and tags
- Complete lineage tracking for all data changes

#### Connection Management

- [`install_ducklake()`](https://tgerke.github.io/ducklake-r/reference/install_ducklake.md) -
  Install/update DuckLake extension
- [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md) -
  Initialize data lake connections
- [`detach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/detach_ducklake.md) -
  Clean up connections
- [`get_ducklake_connection()`](https://tgerke.github.io/ducklake-r/reference/get_ducklake_connection.md) -
  Retrieve the active DuckDB connection

#### Query Execution

- [`ducklake_exec()`](https://tgerke.github.io/ducklake-r/reference/ducklake_exec.md) -
  Execute SQL with automatic assignment handling
- [`show_ducklake_query()`](https://tgerke.github.io/ducklake-r/reference/show_ducklake_query.md) -
  Preview translated SQL queries
- `extract_assignments_from_sql()` - Parse SQL table assignments

#### Backup and Maintenance

- [`backup_ducklake()`](https://tgerke.github.io/ducklake-r/reference/backup_ducklake.md) -
  Create incremental backups
- Support for local and remote backup locations

### Vignettes

- **Getting Started** - Introduction to ducklake workflows
- **Clinical Trial Data Lake** - Industry-specific use case
- **Modifying Tables** - Comprehensive guide to row operations
- **Working with Transactions** - ACID transaction patterns
- **Time Travel Queries** - Historical data access
- **Storage and Backup Management** - Data persistence strategies

### Lifecycle

This package is currently in **experimental** status. The API may change
as we gather feedback from early users, but core functionality is stable
and ready for pilot projects.
