# ducklake development notes

## Scope decisions

- **MotherDuck is out of scope** (decided 2026-08-10). The package targets
  self-hosted DuckLake catalogs (DuckDB, PostgreSQL, SQLite, MySQL): no
  `md:` backend and no MotherDuck-specific attach forms. Revisit only if
  that decision is explicitly reopened.
- **SQL macros are deliberately not wrapped** (2026-08-10). A macro body is
  raw SQL, and dbplyr cannot translate R calls into user-defined macros, so
  a wrapper would add quoting risk without removing any SQL from user code.
  Views (`create_view()`) cover shared logic; `vignette("views-comments-labels")`
  documents the raw `DBI::dbExecute()` escape hatch for macros.

## Reference

- The feature set was cross-checked against "DuckLake: The Definitive
  Guide" (Martin & Monahan, O'Reilly early release 4, chapters 1-4) in
  August 2026. Watch items when later chapters publish: one-command
  Iceberg catalog export (chapter 6), performance-tuning guidance
  (chapter 5).
- DuckLake limits confirmed empirically (August 2026, DuckDB 1.5.1): one
  update/delete action per MERGE statement (`merge_into()` falls back to
  MERGE + DELETE in one transaction), no MERGE RETURNING, `ALTER COLUMN
  SET TYPE` allows widening promotions only, and `ADD COLUMN ... DEFAULT`
  backfills existing rows.
- ATTACH behavior confirmed empirically (2026-08-27): `META_`-prefixed
  ATTACH options are forwarded to the catalog database, so
  `META_ENCRYPTION_KEY` encrypts a duckdb-format catalog
  (`meta_encryption_key` in `attach_ducklake()`). ATTACH cannot take bound
  parameters -- DuckLake re-serializes forwarded options into an internal
  second ATTACH -- so option values interpolate via `quote_sql()`.

## Decisions from the September 2026 review

- **Writes from lazy tables run in-engine** (2026-09-04). `create_table()`
  runs `CREATE TABLE ... AS`; `replace_table()` materializes the query in
  a DuckDB temp table, then rebuilds the target. Column comments are copied
  by name from the query's base tables so labels survive pipelines.
- **DuckLake in-transaction rules, confirmed empirically on DuckDB 1.5.5**
  (2026-09-04): `ALTER TABLE ... SET PARTITIONED BY / SORTED BY` is refused
  on a table holding inlined rows written in the open transaction, so keys
  go on the empty table before the rows are inserted; `set_option()` is
  refused for a table created in the open transaction, so table options
  are re-set after the commit; `CREATE OR REPLACE TABLE` followed by
  `INSERT` in one transaction commits an empty table, so rewrites use
  `DROP` + `CREATE`; a `DEFAULT` accepts plain constants only (no `TRUE`,
  `DATE '...'`, casts); some failed DDL leaves an aborted transaction even
  in autocommit mode, which `db_execute_ddl()` rolls back; DuckDB temp
  tables are transactional, so the rollback handler must run before the
  temp-table cleanup.
- **`replace_table()` and `restore_table_version()` change the table id**
  (DuckLake has no in-place replace); every piece of metadata DuckLake
  keys to the id is captured first and put back.
- **`set_snapshot_metadata()` fills blanks only** unless `overwrite = TRUE`;
  at-commit metadata is the audited path, and an overwrite is an edit to
  the audit trail.
- **Minimum duckdb is 1.5.5** (raised from 1.5.2 on 2026-09-07). DuckLake
  1.0 ships with DuckDB 1.5.2 (the 1.5.1 extension wrote 0.4-format
  catalogs; `automatic_migration` upgrades them); 1.5.5 is where duckdb-r
  settled its extension storage policy and API (`duckdb_storage_status()`,
  `duckdb(home = )`), which `ducklake_extension_available()` uses. The
  1.5.4.x releases carried a package-library extension cache that upstream
  withdrew, and are excluded on purpose.
- **`CHECKPOINT` expires and deletes nothing without a policy**
  (`expire_older_than`, `delete_older_than`); verified 2026-09-04.
- **Schema-qualified names go through `dbplyr::tbl_sql()` with a quoted
  `I()` path**, because the duckdb driver's `tbl()` method wraps any
  dotted name it cannot find as a literal table name in raw SQL, which
  `rows_*()` cannot write to.
- **Windows (`windows_amd64_mingw`, what duckdb-r uses)**: `sqlite_scanner`,
  `httpfs`, `quack`, and `ducklake` exist; `postgres`, `mysql`, `aws`, and
  `azure` do not (checked 2026-09-04 for v1.5.1 and v1.5.5).
- **Extension persistence** (duckdb-r 1.5.5, confirmed 2026-09-07): every
  new `duckdb()` driver resolves one home root, `home` argument →
  `duckdb.home` option → `DUCKDB_R_HOME` → existing `~/.duckdb` →
  interactive one-time offer to create `~/.duckdb` (`askYesNo`, default
  yes) → per-session tempdir; non-interactive sessions get a throttled
  message instead. The R client sets `autoinstall_known_extensions =
  false`, so an explicit `LOAD` never downloads and
  `load_or_install_extension()` is the package's only install path. Local
  development and CI set `DUCKDB_R_HOME`.
- **No load-time extension check or install** (2026-09-07).
  `install_ducklake()` is optional: `attach_ducklake()` installs on first
  use after a message, and duckdb's connect-time prompt handles the durable
  directory. An `.onLoad()`/`.onAttach()` probe would start DuckDB on every
  `library(ducklake)` (dependency loads and `R CMD check` included) to
  answer a question the attach path already answers; a load-time download
  would break CRAN's rule against writing outside the session tempdir
  without consent; a startup message about the directory would fire in
  every session for every user without the setting and duplicate duckdb's
  prompt. `create_ducklake_connection()` deliberately passes no `home`, so
  that prompt is not suppressed.
- **Iceberg interop is documented, not wrapped** (2026-09-04): `COPY FROM
  DATABASE lake TO iceberg_catalog` and `iceberg_to_ducklake()` live in
  DuckDB's iceberg extension; `vignette("loading-data")` shows them. This closes the
  "one-command Iceberg catalog export" watch item above.
- **A commit is confirmed with one line naming its snapshot** (2026-09-06).
  `begin_transaction()` is silent and stashes the lake's current snapshot
  id; `commit_transaction()` reads it again after `COMMIT` and prints
  `Committed snapshot N (author): message`, or says nothing changed when
  the id did not move. DuckLake assigns the id only at commit, and an
  empty or read-only transaction creates no snapshot (confirmed on the
  DuckDB and SQLite catalogs). Limitation: DuckLake has no query for "the
  snapshot this transaction created", so the id is whatever
  `current_snapshot()` returns right after the commit; with concurrent
  writers it can belong to a neighbor's commit that landed in between.
  Functions that commit for themselves (`restore_table_version()`) print
  no second confirmation.
- **Article layout** (2026-09-06): `vignette("ducklake")` (Getting
  Started) is one short linear session; recipes live in topical articles
  (Loading Data; Views, Comments, and Labels; Modifying Tables; Choosing a
  Deployment). New recipes go to those, not to Getting Started.
- **The README follows the Getting Started arc** (2026-09-08): one layered
  session in short headed steps (attach, load, derive, read, rebuild,
  history, detach), commit confirmations visible, printed results collected
  so no temp path shows. The dplyneage section screenshots the lake's
  stitched bronze/silver/gold lineage. Rendering `README.Rmd` needs
  dplyneage (GitHub only), webshot2, and Chrome, none of them package
  dependencies, so `README.Rmd` stays build-ignored and `README.md` is
  rendered locally with `devtools::build_readme()`. The lineage chunk
  renders without a warning only with dplyneage newer than 0.3.1, which
  exempts a model's own sources from its unstitched-model check.
