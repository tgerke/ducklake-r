# cran-comments

## R CMD check results

0 errors | 0 warnings | 0 notes

Checked both with the extension present (all examples, tests, and vignettes
execute) and with it absent (everything gates itself off and the check is
still clean), since the latter is what the check farm will see.

## This release

ducklake 1.0.0 requires duckdb >= 2.0.0, the release that ships DuckLake 1.1.
The package wraps features that exist only in that extension version (nested
schemas, table layout set in DDL, column defaults, catalog format migration),
and DuckDB 2.0 rewrites a DuckDB-file catalog in a storage format that DuckDB
1.5 cannot read, so a lower floor would leave users one write away from a lake
their other clients cannot open. The earlier release keeps working on duckdb
1.5.

One test starts a second R process as a Quack server and attaches a lake
through it. It is skipped on CRAN and on Windows, is gated on the extension
like the rest, and runs in the GitHub Actions matrix on macOS and Linux.

## Test environments

* local macOS (aarch64), R 4.5.2
* win-builder (devel and release)
* mac-builder (release)
* GitHub Actions: macOS, Windows, Ubuntu (devel, release, oldrel-1)

## Notes for reviewers

ducklake wraps the `ducklake` DuckDB extension. That extension is not bundled
with the duckdb R package and is downloaded on first use, so nothing in the
package may assume it is present.

Everything that needs it is gated on `ducklake_extension_available()`, which
opens a throwaway in-memory DuckDB connection and asks `duckdb_extensions()`
whether the extension is already installed, loading it only if so. Reading the
catalog cannot download anything, and loading an already-installed extension
does not reach the network, so the probe never writes to the extension cache in
the user's home directory. Tests also call `skip_on_cran()`.

* Examples use `@examplesIf ducklake_extension_available()` and build their
  lakes under `tempdir()`. On a machine with the extension installed they all
  run; on the check farm they skip.
* `\dontrun{}` is used only where the example cannot run anywhere without
  external infrastructure: a running Quack server, a PostgreSQL or MySQL
  catalog, or a remote URL.
* Vignettes evaluate their chunks only when the same predicate returns `TRUE`.
* Extensions are installed only on the user's behalf and never silently:
  `attach_ducklake()` installs the extension the first time a lake is
  attached, and `install_ducklake()` does so ahead of time. Both print a
  message naming the extension and the destination before any download.
  Nothing is downloaded at package load or by the availability probe.
