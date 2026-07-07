#' Execute a SQL statement on the shared ducklake connection
#'
#' Thin wrapper around [DBI::dbExecute()] against
#' [get_ducklake_connection()], used by every function in the package that
#' runs a statement for its side effects.
#'
#' @param sql A single SQL statement.
#' @param conn A DBI connection; defaults to the shared ducklake connection.
#' @returns The number of rows affected, invisibly.
#' @noRd
db_execute <- function(sql, conn = get_ducklake_connection()) {
  invisible(DBI::dbExecute(conn, sql))
}
