#' List the tables and views in a DuckLake catalog
#'
#' Returns every table and view in the lake as a tidy data frame -- the
#' quick answer to "what is in here?".
#'
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @returns A data frame with one row per object: `schema_name`,
#'   `table_name`, and `type` (`"table"` or `"view"`).
#' @family table operations
#' @export
#'
#' @seealso [get_table_info()] for per-table file statistics,
#'   [get_table_comments()] for stored documentation, [get_ducklake_table()]
#'   to read any listed object.
#'
#' @examples
#' \dontrun{
#' list_ducklake_tables()
#' }
list_ducklake_tables <- function(ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  # duckdb_tables()/duckdb_views() see whatever the attached catalog
  # exposes, so this works unchanged on every catalog backend and respects
  # a snapshot-pinned attach
  DBI::dbGetQuery(
    conn,
    "SELECT schema_name, table_name, 'table' AS type
     FROM duckdb_tables() WHERE database_name = ? AND NOT internal
     UNION ALL
     SELECT schema_name, view_name AS table_name, 'view' AS type
     FROM duckdb_views() WHERE database_name = ? AND NOT internal
     ORDER BY schema_name, type, table_name",
    params = list(ducklake_name, ducklake_name)
  )
}
