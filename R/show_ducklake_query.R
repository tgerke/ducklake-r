#' Show the SQL that would be executed by ducklake operations
#'
#' This function shows the SQL that would be generated and executed by ducklake.
#' This is useful for debugging and understanding what SQL is being sent to DuckDB.
#'
#' @param .data A dplyr query object (tbl_lazy)
#' @param table_name The target table name for the operation. If not provided, will be extracted from the table attribute (set by get_ducklake_table())
#'
#' @return The first argument, invisibly (following show_query convention)
#' @export
#'
#' @examples
#' \dontrun{
#' # Show SQL for an update operation (table name inferred)
#' get_ducklake_table("my_table") |>
#'   mutate(status = "updated") |>
#'   show_ducklake_query()
#' }
show_ducklake_query <- function(.data, table_name = NULL) {
  
  # Extract table name from attribute if not provided
  if (is.null(table_name)) {
    table_name <- attr(.data, "ducklake_table_name", exact = TRUE)
    if (is.null(table_name)) {
      stop("table_name must be provided either as an argument or via get_ducklake_table()")
    }
  }
  cat("\n=== DuckLake SQL Preview ===\n")
  
  # Show main operation SQL
  cat("\n-- Main operation\n")
  sql_string <- update_table(.data, table_name, .quiet = TRUE)
  cat(sql_string, ";\n")
  
  invisible(.data)
}

