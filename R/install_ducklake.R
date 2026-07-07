#' Install the ducklake extension to duckdb
#'
#' Installs the ducklake DuckDB extension and optionally the extensions for
#' alternative catalog backends (postgres, sqlite, mysql). Only needs to be
#' run once per DuckDB version.
#'
#' @param backend Optional character vector of backends to install. The ducklake
#'   extension is always installed. Pass `"postgres"`, `"sqlite"`, and/or
#'   `"mysql"` to install the corresponding backend extensions.
#'
#' @note On Windows the `postgres` and `mysql` extensions are not available
#'   (MinGW toolchain). See [attach_ducklake()] for details.
#'
#' @returns NULL
#' @export
#'
#' @examples
#' \dontrun{
#' install_ducklake()
#' install_ducklake(backend = "postgres")
#' install_ducklake(backend = c("postgres", "sqlite", "mysql"))
#' }
install_ducklake <- function(backend = NULL) {
  # DuckLake v1.0 officially ships with DuckDB 1.5.2, but the ducklake extension
  # is compatible with engine >= 1.5.1 (the version bundled in duckdb R pkg 1.5.1).
  # SELECT version() returns the DuckDB engine version, not the R package version.
  conn <- get_ducklake_connection()
  duckdb_version <- DBI::dbGetQuery(conn, "SELECT version()")[1, 1]
  duckdb_version_parsed <- numeric_version(sub("^v", "", duckdb_version))
  if (duckdb_version_parsed < "1.5.1") {
    cli::cli_abort(
      "DuckLake v1.0 requires DuckDB version 1.5.1 or higher (found {duckdb_version})."
    )
  }

  # the long messages thrown on load for duckplyr are suppressed here
  # TODO: find a better/more global place to do this, since duckplyr used elsewhere
  suppressMessages(duckplyr::db_exec("INSTALL ducklake;"))
  cli::cli_inform("Installed {.pkg ducklake} extension.")

  valid_backends <- c("postgres", "sqlite", "mysql")
  if (!is.null(backend)) {
    backend <- match.arg(backend, valid_backends, several.ok = TRUE)
    for (ext in backend) {
      suppressMessages(duckplyr::db_exec(sprintf("INSTALL %s;", ext)))
      cli::cli_inform("Installed {.pkg {ext}} extension.")
    }
  }

  invisible(NULL)
}
