#' Download the ducklake extension ahead of time
#'
#' [attach_ducklake()] installs the ducklake DuckDB extension the first time
#' it is needed, so most sessions never call this function. It downloads the
#' extension now, and optionally the extensions for the other catalog
#' backends (postgres, sqlite, mysql), for the cases where the download has
#' to happen before a lake is attached: baking a container image, a CI setup
#' step, or a machine that is offline at attach time.
#'
#' @param backend Optional character vector of backends to install. The ducklake
#'   extension is always installed. Pass `"postgres"`, `"sqlite"`, and/or
#'   `"mysql"` to install the corresponding backend extensions.
#'
#' @details
#' Where the extension lands is decided by the duckdb R package (see
#' `?duckdb::duckdb_storage`). Extensions live under a "home" directory: the
#' `duckdb.home` option or `DUCKDB_R_HOME` when set, otherwise `~/.duckdb`
#' when it exists (in an interactive session duckdb offers to create it the
#' first time it connects), otherwise a per-session temporary directory.
#' With the temporary directory the extension is gone when R exits and the
#' next session downloads it again. `install_ducklake()` reports the
#' directory it installed into and says so when that directory is temporary.
#' For scripts and CI, set `DUCKDB_R_HOME` in `~/.Renviron` or the job's
#' environment to a directory that survives the session.
#'
#' @note On Windows the `postgres` and `mysql` extensions are not available
#'   (MinGW toolchain). See [attach_ducklake()] for details.
#'
#' @returns Invisibly, `NULL`. Called for its side effect of installing the
#'   DuckDB extensions into the local extension cache.
#' @family connection management
#' @export
#'
#' @examples
#' \dontrun{
#' install_ducklake()
#' install_ducklake(backend = "postgres")
#' install_ducklake(backend = c("postgres", "sqlite", "mysql"))
#' }
install_ducklake <- function(backend = NULL) {
  # DuckLake 1.0 ships with DuckDB 1.5.2; the extension built for 1.5.1
  # writes the earlier 0.4 catalog format. SELECT version() returns the
  # engine version, not the R package version.
  conn <- get_ducklake_connection()
  duckdb_version <- DBI::dbGetQuery(conn, "SELECT version()")[1, 1]
  if (!duckdb_version_at_least(duckdb_version, "1.5.2")) {
    cli::cli_abort(
      "DuckLake 1.0 requires DuckDB version 1.5.2 or higher (found {duckdb_version})."
    )
  }

  db_execute("INSTALL ducklake;")
  reset_extension_available_cache()
  dir <- extension_directory(conn)
  cli::cli_inform(c(
    "Installed {.pkg ducklake} extension into {.path {dir}}.",
    extension_persistence_hint(dir)
  ))

  valid_backends <- c("postgres", "sqlite", "mysql")
  if (!is.null(backend)) {
    backend <- match.arg(backend, valid_backends, several.ok = TRUE)
    for (ext in backend) {
      db_execute(sprintf("INSTALL %s;", ext))
      cli::cli_inform("Installed {.pkg {ext}} extension.")
    }
  }

  invisible(NULL)
}
