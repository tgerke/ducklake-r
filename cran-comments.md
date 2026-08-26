# cran-comments

## R CMD check results

0 errors | 0 warnings | 1 note

* This is a new release, so the "New submission" note is expected.

Checked both with the extension present (all examples, tests, and vignettes
execute) and with it absent (everything gates itself off and the check is
still clean), since the latter is what the check farm will see.

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
* `install_ducklake()` is the one function that installs the extension, and it
  only does so when the user calls it. Where the package loads an extension on
  the user's behalf, it prints a message naming the extension and the
  destination before any download.
