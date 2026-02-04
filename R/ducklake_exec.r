#' Execute DuckLake operations from dplyr queries
#'
#' @param .data A dplyr query object (tbl_lazy) with accumulated operations
#' @param table_name The target table name for the operation. If not provided, will be extracted from the table attribute (set by get_ducklake_table())
#' @param .quiet Logical, whether to suppress debug output (default TRUE)
#'
#' @return The result from duckplyr::db_exec()
#' @export
#'
#' @details
#' This function automatically detects the type of operation based on dplyr verbs:
#' - Filter-only queries generate DELETE operations (removes rows that DON'T match filter)
#' - Queries with mutate() generate UPDATE operations
#' - Other queries generate INSERT operations
#'
#' @examples
#' \dontrun{
#' # Delete rows that don't match filter (table name inferred)
#' get_ducklake_table("my_table") |>
#'   filter(status == "inactive") |>
#'   ducklake_exec()
#'
#' # Update specific rows (table name inferred)
#' get_ducklake_table("my_table") |>
#'   filter(id == 123) |>
#'   mutate(status = "updated") |>
#'   ducklake_exec()
#'
#' # Or provide table name explicitly
#' tbl(con, "my_table") |>
#'   select(id, name) |>
#'   mutate(computed_field = name * 2) |>
#'   ducklake_exec("my_table")
#' }
ducklake_exec <- function(.data, table_name = NULL, .quiet = TRUE) {
  
  # Extract table name from attribute if not provided
  if (is.null(table_name)) {
    table_name <- attr(.data, "ducklake_table_name", exact = TRUE)
    if (is.null(table_name)) {
      stop("table_name must be provided either as an argument or via get_ducklake_table()")
    }
  }

  if (!.quiet) {
    # Show the original dplyr SQL
    cat("\n=== Original dplyr SQL ===\n")
    print(dplyr::show_query(.data))
  }

  # Generate the DuckLake SQL using update_table
  sql_string <- update_table(.data, table_name, .quiet = TRUE)

  if (!.quiet) {
    cat("\n=== Translated DuckLake SQL ===\n")
    cat(sql_string, "\n")
  }

  # Execute and return result
  result <- duckplyr::db_exec(sql_string)

  if (!.quiet) {
    cat("\nRows affected:", result, "\n")
  }

  return(result)
}
