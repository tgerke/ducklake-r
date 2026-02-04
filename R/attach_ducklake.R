#' Create or attach a ducklake
#'
#' This function is a wrapper for the ducklake [ATTACH](https://ducklake.select/docs/stable/duckdb/usage/connecting) command.
#' It will create a new DuckDB-backed DuckLake if the specified name does not exist, or connect to the existing DuckLake if it does exist.
#' The connection is stored in the package environment and can be closed with detach_ducklake().
#'
#' @param ducklake_name Name for the ducklake file, as in `ducklake:{ducklake_name}.ducklake`
#' @param data_path Optional directory where Parquet files are stored. If not specified, uses the default folder `{ducklake_name}.ducklake.files` in the same directory as the DuckLake itself.
#'
#' @returns NULL
#' @export
#'
#' @seealso [detach_ducklake()] to close the connection
#'
attach_ducklake <- function(ducklake_name, data_path = NULL) {
  # Get or create connection
  conn <- get_ducklake_connection()
  
  # Store the connection in our environment
  set_ducklake_connection(conn)
  
  if (is.null(data_path)) {
    duckplyr::db_exec(sprintf("ATTACH 'ducklake:%s.ducklake' AS %s;", ducklake_name, ducklake_name))
  } else {
    duckplyr::db_exec(sprintf("ATTACH 'ducklake:%s.ducklake' AS %s (DATA_PATH '%s');", ducklake_name, ducklake_name, data_path))
  }

  duckplyr::db_exec(sprintf("USE %s;", ducklake_name))
  
  invisible(NULL)
}
