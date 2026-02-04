# Package environment to store the ducklake connection
.ducklake_env <- new.env(parent = emptyenv())

#' Get the current DuckLake connection
#'
#' @return A DuckDB connection object
#' @keywords internal
get_ducklake_connection <- function() {
  conn <- .ducklake_env$connection
  
  if (is.null(conn)) {
    # Fall back to duckplyr's default connection if no ducklake connection is set
    conn <- duckplyr:::get_default_duckdb_connection()
  }
  
  return(conn)
}

#' Set the DuckLake connection
#'
#' @param conn A DuckDB connection object
#' @keywords internal
set_ducklake_connection <- function(conn) {
  .ducklake_env$connection <- conn
  invisible(conn)
}

#' Detach from a ducklake
#'
#' Closes the DuckDB connection and detaches from the current DuckLake.
#'
#' @param ducklake_name Optional name of the ducklake to detach. If not provided, closes the current connection.
#'
#' @returns NULL
#' @export
#'
#' @examples
#' \dontrun{
#' attach_ducklake("my_ducklake")
#' # ... do work ...
#' detach_ducklake("my_ducklake")
#' }
detach_ducklake <- function(ducklake_name = NULL) {
  if (!is.null(ducklake_name)) {
    # Detach the specific database
    tryCatch({
      duckplyr::db_exec(sprintf("DETACH %s;", ducklake_name))
    }, error = function(e) {
      warning("Could not detach ducklake: ", e$message)
    })
  }
  
  # Clear the stored connection
  conn <- .ducklake_env$connection
  if (!is.null(conn)) {
    tryCatch({
      DBI::dbDisconnect(conn, shutdown = TRUE)
    }, error = function(e) {
      warning("Could not disconnect: ", e$message)
    })
    .ducklake_env$connection <- NULL
  }
  
  invisible(NULL)
}
