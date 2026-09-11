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
- **A commit is confirmed with one line naming its snapshot** (2026-09-06,
  revised 2026-09-11). `begin_transaction()` is silent and stashes what
  this connection last committed; `commit_transaction()` reads it again
  after `COMMIT` and prints `Committed snapshot N (author): message`, or
  says nothing changed when the id did not move, naming the snapshot the
  lake stands at. The id comes from DuckLake's
  `last_committed_snapshot()`, which is connection state: NA until the
  connection commits a change to the lake, then the id of its latest
  writing commit, unmoved by empty commits, rollbacks, and other
  connections' commits (confirmed on DuckDB 1.5.5, extension d8a1881e,
  with the DuckDB and SQLite catalogs). An extension without the function
  falls back to `current_snapshot()`, the lake's newest snapshot, which
  under concurrent writers can be a neighbor's; the 2026-09-06 note that
  no better query existed was wrong. `begin_transaction()` reads the
  stash before `BEGIN`: DuckDB starts the transaction on a catalog at the
  first statement that touches it, and on a SQLite catalog that
  transaction holds the file's shared lock until it ends, so a neighbor's
  commit fails with "database is locked" from that point on. The test in
  `test-transactions.R` stages the neighbor's commits in the window before
  the first lake statement. Functions that commit for themselves
  (`restore_table_version()`) print no second confirmation.
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
- **The clinical article is pkgdown-only** (2026-09-09).
  `vignettes/articles/clinical-trial-datalake.Rmd` is built by pkgdown,
  not R CMD check, so it can use dplyneage (GitHub-only), haven, admiral,
  and pharmaversesdtm, declared in `Config/Needs/website`. Bronze loads an
  XPT transfer written from pharmaversesdtm, because pharmaversesdtm has
  no blank cells and the silver blank-to-`NA` step was a no-op on it; XPT
  read with haven does carry blanks. dplyneage draws the gold-to-results
  hop only: admiral runs on data frames and leaves no query tree, so the
  admiral layers' traceability is ADaM metadata. The article claims no
  "Part 11 compliance"; it maps the snapshot history onto the audit-trail
  elements in FDA's 2024 Q&A (Q12-Q14) and cites ICH E6(R3) section 4.2.2.
- **Snapshot 0 is labeled at attach time, by one conditional UPDATE**
  (2026-09-11). DuckLake writes snapshot 0 itself during ATTACH and
  ignores `set_commit_message()` around it (confirmed on DuckDB 1.5.5:
  `BEGIN; ATTACH ...; CALL lake.set_commit_message(...); COMMIT` leaves it
  NULL), and no ATTACH option or setting supplies a default author. So
  `attach_ducklake()` runs one UPDATE on `ducklake_snapshot_changes`
  (`snapshot_id = 0`, all three fields NULL, exactly one row in
  `ducklake_snapshot`) after creating a lake; the rows-affected count says
  whether the lake was new, with no probe-then-write window. It skips
  read-only and pinned attaches (a pinned attach leaves the metadata
  catalog writable), `create = FALSE`, a local catalog file that existed
  before the ATTACH, and an open transaction (a DuckDB transaction writes
  to one attached database, so a label there blocks the lake writes after
  it). Silent; warns on failure. A read-only ATTACH cannot create a lake,
  so that guard matters only for existing lakes on server catalogs.
- **`ducklake.author` applies at the package's commit points**
  (2026-09-11): `commit_transaction()` (so `with_transaction()` and
  `restore_table_version()`) and the creation snapshot. Not
  `set_snapshot_metadata()`, where the person labeling is not always the
  one who committed, and not autocommit writes, which record no metadata.
  An empty string is refused, so an unset environment variable fails
  loudly instead of recording `""`.
- **`metadata_schema` lives in the lake registry** (2026-09-11). The
  metadata database is hidden from `duckdb_databases()` and
  `duckdb_tables()`, so the schema cannot be discovered after ATTACH;
  `register_lake()` keeps it, and `metadata_prefix()` and
  `get_metadata_table()` read it there. SQLite catalogs refuse
  `METADATA_SCHEMA`.
