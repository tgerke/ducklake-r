# ducklake development notes

## Scope decisions

- **MotherDuck is out of scope** (decided 2026-08-10). The package
  targets self-hosted DuckLake catalogs (DuckDB, PostgreSQL, SQLite,
  MySQL): no `md:` backend and no MotherDuck-specific attach forms.
  Revisit only if that decision is explicitly reopened.
- **SQL macros are deliberately not wrapped** (2026-08-10). A macro body
  is raw SQL, and dbplyr cannot translate R calls into user-defined
  macros, so a wrapper would add quoting risk without removing any SQL
  from user code. Views
  ([`create_view()`](https://tgerke.github.io/ducklake-r/reference/create_view.md))
  cover shared logic;
  [`vignette("views-comments-labels")`](https://tgerke.github.io/ducklake-r/articles/views-comments-labels.md)
  documents the raw
  [`DBI::dbExecute()`](https://dbi.r-dbi.org/reference/dbExecute.html)
  escape hatch for macros.

## Reference

- The feature set was cross-checked against “DuckLake: The Definitive
  Guide” (Martin & Monahan, O’Reilly early release 4, chapters 1-4) in
  August 2026. Watch items when later chapters publish: one-command
  Iceberg catalog export (chapter 6), performance-tuning guidance
  (chapter 5).
- DuckLake limits confirmed empirically (August 2026, DuckDB 1.5.1): one
  update/delete action per MERGE statement
  ([`merge_into()`](https://tgerke.github.io/ducklake-r/reference/merge_into.md)
  falls back to MERGE + DELETE in one transaction), no MERGE RETURNING,
  `ALTER COLUMN SET TYPE` allows widening promotions only, and
  `ADD COLUMN ... DEFAULT` backfills existing rows.
- ATTACH behavior confirmed empirically (2026-08-27): `META_`-prefixed
  ATTACH options are forwarded to the catalog database, so
  `META_ENCRYPTION_KEY` encrypts a duckdb-format catalog
  (`meta_encryption_key` in
  [`attach_ducklake()`](https://tgerke.github.io/ducklake-r/reference/attach_ducklake.md)).
  ATTACH cannot take bound parameters – DuckLake re-serializes forwarded
  options into an internal second ATTACH – so option values interpolate
  via
  [`quote_sql()`](https://tgerke.github.io/ducklake-r/reference/quote_sql.md).

## Decisions from the September 2026 review

- **Writes from lazy tables run in-engine** (2026-09-04).
  [`create_table()`](https://tgerke.github.io/ducklake-r/reference/create_table.md)
  runs `CREATE TABLE ... AS`;
  [`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
  materializes the query in a DuckDB temp table, then rebuilds the
  target. Column comments are copied by name from the query’s base
  tables so labels survive pipelines.
- **DuckLake in-transaction rules, confirmed empirically on DuckDB
  1.5.5** (2026-09-04): `ALTER TABLE ... SET PARTITIONED BY / SORTED BY`
  is refused on a table holding inlined rows written in the open
  transaction, so keys go on the empty table before the rows are
  inserted; `set_option()` is refused for a table created in the open
  transaction, so table options are re-set after the commit;
  `CREATE OR REPLACE TABLE` followed by `INSERT` in one transaction
  commits an empty table, so rewrites use `DROP` + `CREATE`; a `DEFAULT`
  accepts plain constants only (no `TRUE`, `DATE '...'`, casts); some
  failed DDL leaves an aborted transaction even in autocommit mode,
  which `db_execute_ddl()` rolls back; DuckDB temp tables are
  transactional, so the rollback handler must run before the temp-table
  cleanup.
- **[`replace_table()`](https://tgerke.github.io/ducklake-r/reference/replace_table.md)
  and
  [`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md)
  change the table id** (DuckLake has no in-place replace); every piece
  of metadata DuckLake keys to the id is captured first and put back.
- **[`set_snapshot_metadata()`](https://tgerke.github.io/ducklake-r/reference/set_snapshot_metadata.md)
  fills blanks only** unless `overwrite = TRUE`; at-commit metadata is
  the audited path, and an overwrite is an edit to the audit trail.
- **Minimum duckdb is 1.5.2**, the release that ships DuckLake 1.0. The
  extension for 1.5.1 wrote 0.4-format catalogs; `automatic_migration`
  upgrades them.
- **`CHECKPOINT` expires and deletes nothing without a policy**
  (`expire_older_than`, `delete_older_than`); verified 2026-09-04.
- **Schema-qualified names go through
  [`dbplyr::tbl_sql()`](https://dbplyr.tidyverse.org/reference/tbl_sql.html)
  with a quoted [`I()`](https://rdrr.io/r/base/AsIs.html) path**,
  because the duckdb driver’s `tbl()` method wraps any dotted name it
  cannot find as a literal table name in raw SQL, which `rows_*()`
  cannot write to.
- **Windows (`windows_amd64_mingw`, what duckdb-r uses)**:
  `sqlite_scanner`, `httpfs`, `quack`, and `ducklake` exist; `postgres`,
  `mysql`, `aws`, and `azure` do not (checked 2026-09-04 for v1.5.1 and
  v1.5.5).
- **Extension persistence**: duckdb-r 1.5.2+ keeps extensions in a
  per-session temp directory unless `DUCKDB_R_HOME` (or `duckdb.home`,
  or an existing `~/.duckdb`) is set; local development and CI set
  `DUCKDB_R_HOME`.
- **Iceberg interop is documented, not wrapped** (2026-09-04):
  `COPY FROM DATABASE lake TO iceberg_catalog` and
  `iceberg_to_ducklake()` live in DuckDB’s iceberg extension;
  [`vignette("loading-data")`](https://tgerke.github.io/ducklake-r/articles/loading-data.md)
  shows them. This closes the “one-command Iceberg catalog export” watch
  item above.
- **A commit is confirmed with one line naming its snapshot**
  (2026-09-06).
  [`begin_transaction()`](https://tgerke.github.io/ducklake-r/reference/begin_transaction.md)
  is silent and stashes the lake’s current snapshot id;
  [`commit_transaction()`](https://tgerke.github.io/ducklake-r/reference/commit_transaction.md)
  reads it again after `COMMIT` and prints
  `Committed snapshot N (author): message`, or says nothing changed when
  the id did not move. DuckLake assigns the id only at commit, and an
  empty or read-only transaction creates no snapshot (confirmed on the
  DuckDB and SQLite catalogs). Limitation: DuckLake has no query for
  “the snapshot this transaction created”, so the id is whatever
  `current_snapshot()` returns right after the commit; with concurrent
  writers it can belong to a neighbor’s commit that landed in between.
  Functions that commit for themselves
  ([`restore_table_version()`](https://tgerke.github.io/ducklake-r/reference/restore_table_version.md))
  print no second confirmation.
- **Article layout** (2026-09-06):
  [`vignette("ducklake")`](https://tgerke.github.io/ducklake-r/articles/ducklake.md)
  (Getting Started) is one short linear session; recipes live in topical
  articles (Loading Data; Views, Comments, and Labels; Modifying Tables;
  Choosing a Deployment). New recipes go to those, not to Getting
  Started.
