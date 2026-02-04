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
#' @param ducklake_name Character string, name of the ducklake database
#'
#' @returns A DuckLake table of class `tbl_duckdb_connection`
#' @export
#'
get_metadata_table <- function(tbl_name, ducklake_name) {
  metadata_tbl_name <- paste0("__ducklake_metadata_", ducklake_name, ".", tbl_name)
  return(get_ducklake_table(metadata_tbl_name))
}
