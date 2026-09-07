#' Is the ducklake DuckDB extension usable?
#'
#' Reports whether the `ducklake` DuckDB extension is already installed and
#' can be loaded. The check opens a throwaway in-memory DuckDB connection,
#' asks DuckDB's extension catalog whether the extension is installed, and
#' loads it only if so. Reading the catalog cannot download anything, and
#' loading an installed extension does not reach the network, so the probe
#' never downloads and never writes to the extension directory. The
#' connection is opened on the directory duckdb would resolve for a new
#' connection, so an existing `~/.duckdb` is found, and the offer duckdb
#' makes to create one is not triggered. The package's own connection is
#' left untouched.
#'
#' Use it to guard code that should degrade gracefully when the extension is
#' absent: examples, vignettes, tests, and conditional branches in scripts.
#' The package's own examples are gated on it. There is no need to call it
#' before [attach_ducklake()], which installs the extension the first time it
#' is needed; [install_ducklake()] downloads it ahead of time.
#'
#' A `FALSE` on a machine where the extension was installed earlier usually
#' means it went into a temporary directory. duckdb keeps extensions under a
#' "home" directory: the `duckdb.home` option or `DUCKDB_R_HOME` when set,
#' otherwise `~/.duckdb` when it exists (in an interactive session duckdb
#' offers to create it the first time it connects), otherwise a per-session
#' temporary directory. See `?duckdb::duckdb_storage`.
#'
#' The result is cached for the rest of the session, since spinning up DuckDB
#' to re-answer the same question is wasteful when dozens of examples ask it.
#' [install_ducklake()] and a first-use install by [attach_ducklake()] clear
#' the cache, so a `FALSE` answer becomes `TRUE` as soon as the extension is
#' there.
#'
#' @returns A single logical: `TRUE` when the extension loads, `FALSE`
#'   otherwise (including when a connection cannot be opened).
#' @family connection management
#' @export
#'
#' @examples
#' if (ducklake_extension_available()) {
#'   message("ducklake extension is ready to use")
#' } else {
#'   message("attach_ducklake() will download it on first use")
#' }
ducklake_extension_available <- function() {
  cached <- .ducklake_env$ext_available
  if (!is.null(cached)) {
    return(cached)
  }

  ok <- tryCatch(
    {
      # Open the probe on the home directory duckdb would resolve for a new
      # connection. Given a `home`, duckdb() neither offers to create
      # ~/.duckdb nor creates anything itself (see ?duckdb::duckdb_storage).
      home <- storage_home_root(duckdb::duckdb_storage_status())
      con <- DBI::dbConnect(duckdb::duckdb(home = home))
      on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
      # Ask the catalog rather than attempting LOAD, so the answer does not
      # depend on the connection's autoinstall setting: reading
      # duckdb_extensions() cannot download anything.
      installed <- DBI::dbGetQuery(
        con,
        "SELECT installed FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
      )
      if (nrow(installed) != 1 || !isTRUE(installed$installed[1])) {
        FALSE
      } else {
        # Already installed, so this LOAD cannot reach the network.
        DBI::dbExecute(con, "LOAD ducklake;")
        TRUE
      }
    },
    error = function(e) FALSE
  )

  .ducklake_env$ext_available <- ok
  ok
}

#' The home directory duckdb would use for a new connection
#'
#' @param status The data frame from [duckdb::duckdb_storage_status()]: one
#'   row per kind of on-disk state, with the `directory` for `"extensions"`
#'   at `<home>/extensions`.
#' @returns The `<home>` path as a string.
#' @noRd
storage_home_root <- function(status) {
  dir <- status$directory[status$kind == "extensions"]
  if (length(dir) != 1 || is.na(dir) || !nzchar(dir)) {
    cli::cli_abort(
      "Could not read the extension directory from {.fn duckdb::duckdb_storage_status}."
    )
  }
  dirname(dir)
}

#' Forget a cached extension-availability answer
#'
#' @returns Invisibly, `NULL`.
#' @noRd
reset_extension_available_cache <- function() {
  .ducklake_env$ext_available <- NULL
  invisible(NULL)
}
