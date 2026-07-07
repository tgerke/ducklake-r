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
#' @family connection management
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
#' @family connection management
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
  .ducklake_env$home_db <- tryCatch(
    DBI::dbGetQuery(conn, "SELECT current_database() AS db")$db,
    error = function(e) NULL
  )
  invisible(conn)
}

#' Switch the session back to the connection's own catalog
#'
#' Detaching the currently `USE`d database is a DuckDB error, so callers must
#' switch away first. Note the previous implementation ran `USE memory;`,
#' which silently fails on file-backed connections -- the default catalog is
#' named after the database file, not `memory`.
#'
#' @noRd
use_home_database <- function(conn) {
  home <- .ducklake_env$home_db
  if (is.null(home) || !nzchar(home)) {
    return(invisible(FALSE))
  }
  tryCatch({
    DBI::dbExecute(conn, sprintf("USE %s;", quote_ident(home, conn)))
    invisible(TRUE)
  }, error = function(e) invisible(FALSE))
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

  # Remember the connection's own catalog so detach_ducklake() can switch
  # back to it ("USE memory" only works for in-memory connections)
  .ducklake_env$home_db <- tryCatch(
    DBI::dbGetQuery(conn, "SELECT current_database() AS db")$db,
    error = function(e) NULL
  )

  if (!isTRUE(.ducklake_env$finalizer_registered)) {
    reg.finalizer(
      .ducklake_env,
      function(e) {
        if (!is.null(e$conn) && isTRUE(e$conn_owned)) {
          still_open <- tryCatch(DBI::dbIsValid(e$conn), error = function(err) FALSE)
          if (still_open) {
            tryCatch(
              suppressWarnings(DBI::dbDisconnect(e$conn, shutdown = TRUE)),
              error = function(err) NULL
            )
          }
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

#' Record an attached lake's backend and connection string
#'
#' Attached lakes are tracked per name so that sessions with several lakes
#' attached at once (possibly on different backends) resolve backend-specific
#' behaviour correctly.
#'
#' @noRd
register_lake <- function(ducklake_name, backend, catalog_connection_string = NULL) {
  if (is.null(.ducklake_env$lakes)) {
    .ducklake_env$lakes <- list()
  }
  .ducklake_env$lakes[[ducklake_name]] <- list(
    backend = backend,
    catalog_connection_string = catalog_connection_string
  )
  invisible(NULL)
}

#' Forget a lake's registry entry (all entries when name is NULL)
#' @noRd
unregister_lake <- function(ducklake_name = NULL) {
  if (is.null(ducklake_name)) {
    .ducklake_env$lakes <- list()
  } else {
    .ducklake_env$lakes[[ducklake_name]] <- NULL
  }
  invisible(NULL)
}

#' Get the catalog backend type of an attached lake
#'
#' @param ducklake_name Name of the lake to look up. When `NULL` (the
#'   default), the lake the session is currently `USE`ing is looked up.
#'
#' @returns One of `"duckdb"`, `"postgres"`, `"sqlite"`, or `"mysql"`.
#'   Defaults to `"duckdb"` when the lake is unknown.
#' @family connection management
#' @export
#'
#' @examples
#' \dontrun{
#' attach_ducklake("my_lake", lake_path = "~/data/lake")
#' get_ducklake_backend()
#' #> [1] "duckdb"
#'
#' # With several lakes attached, look one up by name
#' get_ducklake_backend("my_sqlite_lake")
#' }
get_ducklake_backend <- function(ducklake_name = NULL) {
  lakes <- .ducklake_env$lakes
  if (is.null(lakes) || length(lakes) == 0) {
    return("duckdb")
  }

  if (is.null(ducklake_name)) {
    conn <- .ducklake_env$conn
    if (!is.null(conn)) {
      ducklake_name <- tryCatch(
        DBI::dbGetQuery(conn, "SELECT current_database() AS db")$db,
        error = function(e) NULL
      )
    }
  }

  entry <- if (!is.null(ducklake_name)) lakes[[ducklake_name]] else NULL
  if (!is.null(entry)) {
    return(entry$backend)
  }

  # Unknown name: fall back to the single registered lake if unambiguous
  if (length(lakes) == 1) {
    return(lakes[[1]]$backend)
  }
  "duckdb"
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
#' @family connection management
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
      # DuckDB refuses to DETACH the database currently in use, so switch
      # back to the connection's own catalog first
      use_home_database(conn)

      # Detach the user-facing database and its metadata catalog,
      # tolerating either one already being gone
      tryCatch(
        DBI::dbExecute(conn, sprintf("DETACH %s;", quote_ident(ducklake_name, conn))),
        error = function(e) NULL
      )
      metadata_name <- sprintf("__ducklake_metadata_%s", ducklake_name)
      tryCatch(
        DBI::dbExecute(conn, sprintf("DETACH %s;", quote_ident(metadata_name, conn))),
        error = function(e) NULL
      )
    }

    if (shutdown) {
      close_ducklake_connection()
    }
  }

  # A full shutdown detaches everything; otherwise only forget this lake
  unregister_lake(if (shutdown) NULL else ducklake_name)

  invisible(NULL)
}
