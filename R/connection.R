# Package environment to store the ducklake connection
.ducklake_env <- new.env(parent = emptyenv())

#' Get the current DuckLake connection
#'
#' This function retrieves the active DuckLake connection. If no connection
#' has been explicitly set via \code{set_ducklake_connection()}, it falls back
#' to duckplyr's default DuckDB connection for seamless integration.
#'
#' @return A DuckDB connection object
#' @export
#'
#' @note This function uses \code{duckplyr:::get_default_duckdb_connection()}
#' as a fallback. While this is an unexported function from duckplyr, it is
#' necessary for proper integration with the duckplyr ecosystem when no
#' explicit ducklake connection is set.
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
  conn <- .ducklake_env$connection
  if (!is.null(conn)) {
    # If a ducklake name is provided, try to detach it first
    if (!is.null(ducklake_name)) {
      tryCatch({
        # First detach the user-facing database
        DBI::dbExecute(conn, sprintf("DETACH %s;", ducklake_name))
        
        # Also detach the metadata catalog that DuckLake creates
        metadata_name <- sprintf("__ducklake_metadata_%s", ducklake_name)
        DBI::dbExecute(conn, sprintf("DETACH %s;", metadata_name))
      }, error = function(e) {
        # Ignore errors if database is not attached
      })
    }
    
    # Close the connection
    tryCatch({
      DBI::dbDisconnect(conn, shutdown = TRUE)
    }, error = function(e) {
      warning("Could not disconnect: ", e$message)
    })
    .ducklake_env$connection <- NULL
  }
  
  invisible(NULL)
}
