#' Get a DuckLake table
#'
#' @param tbl_name Character string, name of the table to retrieve
#'
#' @returns A DuckLake table of class `tbl_duckdb_connection` with the table name stored as an attribute
#' @export
#'
get_ducklake_table <- function(tbl_name) {
  tbl <- dplyr::tbl(get_ducklake_connection(), tbl_name)
  attr(tbl, "ducklake_table_name") <- tbl_name
  return(tbl)
}

#' Get a DuckLake metadata table
#'
#' @param tbl_name Character string, name of the table to retrieve
#' @param ducklake_name Character string, name of the ducklake database (optional, defaults to current active ducklake)
#'
#' @returns A DuckLake table of class `tbl_duckdb_connection`
#' @export
#'
get_metadata_table <- function(tbl_name, ducklake_name = NULL) {
  # If ducklake_name not provided, try to infer from current database
  if (is.null(ducklake_name)) {
    conn <- get_ducklake_connection()
    tryCatch({
      current_db <- DBI::dbGetQuery(conn, "SELECT current_database() as db")$db
      if (!is.null(current_db) && current_db != "") {
        ducklake_name <- current_db
      }
    }, error = function(e) {
      stop("Could not determine ducklake_name. Please provide it explicitly.")
    })
  }
  
  # Metadata tables are in the __ducklake_metadata_[ducklake_name] database, main schema
  metadata_tbl_name <- paste0("__ducklake_metadata_", ducklake_name, ".main.", tbl_name)
  return(get_ducklake_table(metadata_tbl_name))
}
