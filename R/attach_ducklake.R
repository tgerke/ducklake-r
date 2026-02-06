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
  # Get or create connection
  conn <- get_ducklake_connection()
  
  # Store the connection in our environment
  set_ducklake_connection(conn)
  
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
