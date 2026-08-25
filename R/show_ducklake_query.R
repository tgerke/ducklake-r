#' Show the SQL that would be executed by ducklake operations
#'
#' This function shows the SQL that would be generated and executed by ducklake.
#' This is useful for debugging and understanding what SQL is being sent to DuckDB.
#'
#' @param .data A dplyr query object (tbl_lazy)
#' @param table_name The target table name for the operation. If not provided, will be extracted from the table attribute (set by get_ducklake_table())
#'
#' @returns The first argument, invisibly (following show_query convention)
#' @family table operations
#' @export
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("sql_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("sql_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' # Show SQL for an update operation (table name inferred)
#' get_ducklake_table("cars") |>
#'   dplyr::mutate(gear = 5) |>
#'   show_ducklake_query()
#'
#' detach_ducklake("sql_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
show_ducklake_query <- function(.data, table_name = NULL) {
  
  # Extract table name from attribute if not provided
  if (is.null(table_name)) {
    table_name <- attr(.data, "ducklake_table_name", exact = TRUE)
    if (is.null(table_name)) {
      cli::cli_abort("{.arg table_name} must be provided either as an argument or via {.fn get_ducklake_table}.")
    }
  }
  cat("\n=== DuckLake SQL Preview ===\n")
  
  # Show main operation SQL without executing it
  cat("\n-- Main operation\n")
  sql_string <- update_table(.data, table_name, .quiet = TRUE, .execute = FALSE)
  cat(sql_string, ";\n")
  
  invisible(.data)
}

