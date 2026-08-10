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
  cover shared logic; the cookbook vignette documents the raw
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
