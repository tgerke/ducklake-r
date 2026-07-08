#' Get file statistics for the tables in a lake
#'
#' Returns per-table storage statistics from the DuckLake catalog: how many
#' Parquet data files each table has and their total size, plus the same for
#' delete files.
#'
#' @param table_name Optional table name. When provided, only that table's
#'   row is returned.
#' @param ducklake_name Name of the attached DuckLake catalog. If `NULL`, the
#'   current database is used.
#' @param conn Optional DuckDB connection object. If not provided, uses the
#'   default ducklake connection.
#'
#' @returns A data frame with one row per table and columns `table_name`,
#'   `schema_id`, `table_id`, `table_uuid`, `file_count`, `file_size_bytes`,
#'   `delete_file_count`, and `delete_file_size_bytes`.
#' @family maintenance
#' @export
#'
#' @details
#' These statistics are the raw material for storage maintenance decisions:
#' many small files are worth compacting with [merge_adjacent_files()], and a
#' growing delete-file share is a sign to run [rewrite_data_files()]. Rows
#' that are still inlined in the catalog (see [set_inlining_row_limit()]) are
#' not in any Parquet file yet, so small recent writes may not show up in the
#' counts until [flush_inlined_data()] writes them out.
#'
#' This wraps DuckLake's `ducklake_table_info()` function.
#'
#' @seealso [plot_table_files()], [merge_adjacent_files()],
#'   [rewrite_data_files()]
#'
#' @examples
#' \dontrun{
#' # File statistics for every table in the lake
#' get_table_info()
#'
#' # Just one table
#' get_table_info("my_table")
#' }
get_table_info <- function(table_name = NULL, ducklake_name = NULL, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  result <- DBI::dbGetQuery(
    conn,
    sprintf("SELECT * FROM ducklake_table_info(%s)", quote_sql(ducklake_name))
  )

  if (!is.null(table_name)) {
    result <- result[result$table_name == table_name, ]
    rownames(result) <- NULL
  }
  result
}
