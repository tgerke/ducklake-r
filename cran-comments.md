# cran-comments

## R CMD check results

0 errors | 0 warnings | 0 notes

* This is a new release.

## Test environments

* local macOS (aarch64), R release
* win-builder (devel and release)
* mac-builder (release)

## Notes for reviewers

* ducklake wraps the 'ducklake' DuckDB extension, which is not bundled with
  the duckdb R package and needs network access to install on first use.
  Everything that depends on the extension is guarded: tests skip via
  `skip_on_cran()` (through a shared helper), vignette chunks evaluate only
  when the extension can be loaded, and all examples are wrapped in
  `\dontrun{}`.
