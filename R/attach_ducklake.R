#' Create or attach a ducklake
#'
#' This function is a wrapper for the ducklake [ATTACH](https://ducklake.select/docs/stable/duckdb/usage/connecting) command.
#' It will create a new DuckDB-backed DuckLake if the specified name does not exist, or connect to the existing DuckLake if it does exist.
#' The connection is stored in the package environment and can be closed with detach_ducklake().
#'
#' @param ducklake_name Name for the ducklake file, as in `ducklake:{ducklake_name}.ducklake`
#' @param lake_path Optional directory path for the ducklake. If specified, both the ducklake database file and Parquet data files will be stored in this location. If not specified, the ducklake is created in the current working directory with data files in `{ducklake_name}.ducklake.files`.
#'
#' @returns NULL
#' @export
#'
#' @seealso [detach_ducklake()] to close the connection
#'
attach_ducklake <- function(ducklake_name, lake_path = NULL) {
  # Get connection - if it's invalid/closed, create a new one
  conn <- get_ducklake_connection()
  
  # Check if connection is valid, if not create a new one
  is_valid <- tryCatch({
    DBI::dbIsValid(conn)
  }, error = function(e) FALSE)
  
  if (!is_valid) {
    # Create a completely new DuckDB connection
    conn <- DBI::dbConnect(duckdb::duckdb())
    set_ducklake_connection(conn)
  }
  
  # Check if this ducklake is already attached to avoid conflicts
  # Query the list of attached databases
  attached <- tryCatch({
    DBI::dbGetQuery(conn, "SELECT database_name FROM duckdb_databases();")$database_name
  }, error = function(e) character(0))
  
  if (ducklake_name %in% attached) {
    # Already attached - just switch to it
    duckplyr::db_exec(sprintf("USE %s;", ducklake_name))
    return(invisible(NULL))
  }
  
  if (is.null(lake_path)) {
    duckplyr::db_exec(sprintf("ATTACH 'ducklake:%s.ducklake' AS %s;", ducklake_name, ducklake_name))
  } else {
    # Construct full path to ducklake file
    ducklake_path <- file.path(lake_path, paste0(ducklake_name, ".ducklake"))
    duckplyr::db_exec(sprintf("ATTACH 'ducklake:%s' AS %s (DATA_PATH '%s');", ducklake_path, ducklake_name, lake_path))
  }

  duckplyr::db_exec(sprintf("USE %s;", ducklake_name))
  
  invisible(NULL)
}
