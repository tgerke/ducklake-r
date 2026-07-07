# Package environment storing the shared connection and per-lake metadata
.ducklake_env <- new.env(parent = emptyenv())

#' Get the DuckDB connection used by ducklake
#'
#' Returns the DuckDB connection that all ducklake functions share. The first
#' call creates the connection automatically, so you never need to set one up
#' yourself. If you want ducklake to use a connection you have created (for
#' example, one shared with other tools), register it first with
#' [set_ducklake_connection()].
#'
#' @details
#' The automatically created connection is backed by a temporary database file
#' (not `:memory:`) with a spill directory configured, so larger-than-memory
#' operations work out of the box. It is closed automatically when the R
#' session ends.
#'
#' @returns A DuckDB connection object (a `duckdb_connection`).
#' @export
#'
#' @examples
#' \dontrun{
#' conn <- get_ducklake_connection()
#' DBI::dbGetQuery(conn, "SELECT version()")
#' }
get_ducklake_connection <- function() {
  conn <- .ducklake_env$conn
  if (!is.null(conn)) {
    is_valid <- tryCatch(DBI::dbIsValid(conn), error = function(e) FALSE)
    if (is_valid) {
      return(conn)
    }
  }

  conn <- create_ducklake_connection()
  .ducklake_env$conn <- conn
  .ducklake_env$conn_owned <- TRUE
  conn
}

#' Use your own DuckDB connection with ducklake
#'
#' By default, ducklake creates and manages its own DuckDB connection. Call
#' this function to make ducklake use a connection you have created instead --
#' for example, a connection you share with duckplyr or other DBI-based tools,
#' or one configured with custom DuckDB settings.
#'
#' @param conn A live DuckDB connection created with
#'   [DBI::dbConnect()][DBI::dbConnect] and [duckdb::duckdb()].
#'
#' @details
#' ducklake never closes a connection you supply: [detach_ducklake()] with
#' `shutdown = TRUE` and the end-of-session cleanup only shut down connections
#' that ducklake created itself. Closing your connection remains your
#' responsibility.
#'
#' If ducklake was already managing its own connection, that connection is
#' shut down before yours is registered.
#'
#' @returns The connection, invisibly.
#' @export
#'
#' @seealso [get_ducklake_connection()]
#'
#' @examples
#' \dontrun{
#' conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = "my_analysis.duckdb")
#' set_ducklake_connection(conn)
#' attach_ducklake("my_lake", lake_path = "~/lakes/my_lake")
#' }
set_ducklake_connection <- function(conn) {
  if (!inherits(conn, "duckdb_connection")) {
    cli::cli_abort(c(
      "{.arg conn} must be a DuckDB connection.",
      "i" = "Create one with {.code DBI::dbConnect(duckdb::duckdb())}."
    ))
  }
  if (!DBI::dbIsValid(conn)) {
    cli::cli_abort("{.arg conn} is not a valid (open) connection.")
  }

  # Shut down a connection we own before replacing it; never touch a
  # previously injected user connection.
  close_ducklake_connection(warn_not_owned = FALSE)

  .ducklake_env$conn <- conn
  .ducklake_env$conn_owned <- FALSE
  invisible(conn)
}

#' Create the package-owned DuckDB connection
#'
#' Mirrors the connection setup duckplyr uses for its default connection: a
#' temporary file-backed database (so DuckDB can spill to disk) with
#' `temp_directory` pointed at the same folder.
#'
#' @returns A new DuckDB connection.
#' @noRd
create_ducklake_connection <- function() {
  dbroot <- Sys.getenv("DUCKLAKE_TEMP_DIR", file.path(tempdir(), "ducklake"))
  dir.create(dbroot, recursive = TRUE, showWarnings = FALSE)
  dbdir <- tempfile("ducklake", tmpdir = dbroot, fileext = ".duckdb")

  conn <- DBI::dbConnect(duckdb::duckdb(dbdir = dbdir))
  DBI::dbExecute(conn, sprintf("PRAGMA temp_directory='%s'", dbroot))

  if (!isTRUE(.ducklake_env$finalizer_registered)) {
    reg.finalizer(
      .ducklake_env,
      function(e) {
        if (!is.null(e$conn) && isTRUE(e$conn_owned)) {
          tryCatch(
            DBI::dbDisconnect(e$conn, shutdown = TRUE),
            error = function(err) NULL
          )
        }
      },
      onexit = TRUE
    )
    .ducklake_env$finalizer_registered <- TRUE
  }

  conn
}

#' Shut down the package-owned connection
#'
#' Disconnects and releases file locks if (and only if) ducklake owns the
#' current connection. A user-supplied connection (registered with
#' [set_ducklake_connection()]) is left untouched. The next call to
#' [get_ducklake_connection()] lazily creates a fresh connection.
#'
#' @param warn_not_owned Warn when the connection is user-supplied and
#'   therefore not shut down.
#' @returns `TRUE` if a connection was shut down, `FALSE` otherwise.
#' @noRd
close_ducklake_connection <- function(warn_not_owned = TRUE) {
  conn <- .ducklake_env$conn
  if (is.null(conn)) {
    return(invisible(FALSE))
  }

  if (!isTRUE(.ducklake_env$conn_owned)) {
    if (warn_not_owned) {
      cli::cli_warn(c(
        "The active connection was supplied via {.fn set_ducklake_connection} and was not shut down.",
        "i" = "Close it yourself with {.code DBI::dbDisconnect(conn, shutdown = TRUE)} when you are done."
      ))
    }
    return(invisible(FALSE))
  }

  is_valid <- tryCatch(DBI::dbIsValid(conn), error = function(e) FALSE)
  if (is_valid) {
    tryCatch(
      DBI::dbDisconnect(conn, shutdown = TRUE),
      error = function(e) cli::cli_warn("Could not shut down connection: {e$message}")
    )
  }

  .ducklake_env$conn <- NULL
  .ducklake_env$conn_owned <- NULL
  invisible(TRUE)
}

#' Get the current catalog backend type
#'
#' @returns One of `"duckdb"`, `"postgres"`, `"sqlite"`, or `"mysql"`.
#'   Defaults to `"duckdb"` when no backend has been set.
#' @export
get_ducklake_backend <- function() {
  backend <- .ducklake_env$backend
  if (is.null(backend)) "duckdb" else backend
}

#' Detach from a ducklake
#'
#' Detaches the DuckLake database but keeps the DuckDB connection alive by
#' default. Use `shutdown = TRUE` to also close the connection and release
#' file locks.
#'
#' @param ducklake_name Optional name of the ducklake to detach.
#' @param shutdown If `TRUE`, shut down the DuckDB connection after detaching.
#'   Only applies to the connection ducklake created itself; a connection
#'   registered with [set_ducklake_connection()] is never closed for you.
#'
#' @returns NULL
#' @export
#'
#' @examples
#' \dontrun{
#' attach_ducklake("my_ducklake", lake_path = "path/to/lake")
#' # ... do work ...
#' detach_ducklake("my_ducklake")
#'
#' # Full shutdown when completely done
#' detach_ducklake("my_ducklake", shutdown = TRUE)
#' }
detach_ducklake <- function(ducklake_name = NULL, shutdown = FALSE) {
  conn <- .ducklake_env$conn

  is_valid <- !is.null(conn) &&
    tryCatch(DBI::dbIsValid(conn), error = function(e) FALSE)

  if (is_valid) {
    if (!is.null(ducklake_name)) {
      tryCatch({
        # Detach the user-facing database and its metadata catalog
        DBI::dbExecute(conn, sprintf("DETACH %s;", ducklake_name))
        metadata_name <- sprintf("__ducklake_metadata_%s", ducklake_name)
        DBI::dbExecute(conn, sprintf("DETACH %s;", metadata_name))
      }, error = function(e) {
        # Ignore errors if database is not attached
      })

      # Switch back to in-memory so subsequent queries don't target
      # the detached lake
      tryCatch(DBI::dbExecute(conn, "USE memory;"), error = function(e) NULL)
    }

    if (shutdown) {
      close_ducklake_connection()
    }
  }

  .ducklake_env$backend <- NULL
  .ducklake_env$catalog_connection_string <- NULL

  invisible(NULL)
}
