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
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("list_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("list_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' list_ducklake_tables()
#'
#' detach_ducklake("list_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
list_ducklake_tables <- function(ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  # duckdb_tables()/duckdb_views() see whatever the attached catalog
  # exposes, so this works unchanged on every catalog backend and respects
  # a snapshot-pinned attach; the full schema path comes from the CTE
  DBI::dbGetQuery(
    conn,
    sprintf(
      "WITH RECURSIVE %s
       SELECT sp.schema_path AS schema_name, t.table_name, 'table' AS type
       FROM duckdb_tables() t
       JOIN schema_paths sp ON t.schema_oid = sp.oid
       WHERE t.database_name = ? AND NOT t.internal
       UNION ALL
       SELECT sp.schema_path, v.view_name AS table_name, 'view' AS type
       FROM duckdb_views() v
       JOIN schema_paths sp ON v.schema_oid = sp.oid
       WHERE v.database_name = ? AND NOT v.internal
       ORDER BY schema_name, type, table_name",
      catalog_schema_paths_cte(ducklake_name)
    ),
    params = list(ducklake_name, ducklake_name)
  )
}
