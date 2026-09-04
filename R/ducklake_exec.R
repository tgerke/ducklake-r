#' Execute DuckLake operations from dplyr queries
#'
#' @param .data A dplyr query object (tbl_lazy) with accumulated operations
#' @param table_name The target table name for the operation. If not provided, will be extracted from the table attribute (set by get_ducklake_table())
#' @param .quiet Logical, whether to suppress the SQL trace (default TRUE).
#'   With `.quiet = FALSE` the original dplyr SQL, the translated statement,
#'   and the number of rows affected are emitted as messages.
#'
#' @returns The result from db_execute()
#' @family table operations
#' @export
#'
#' @details
#' This function automatically detects the type of operation based on dplyr verbs:
#' - Filter-only queries on `table_name` generate DELETE operations
#'   (removes rows that DON'T match filter)
#' - Queries with mutate() on `table_name` generate UPDATE operations
#' - Reads from *other* tables generate INSERT operations, appending their
#'   result into `table_name` with columns matched by name; `filter()` and
#'   joins are fine here, since the whole query just feeds the INSERT
#'
#' A plain read from `table_name` itself is refused, since inserting a
#' table's own rows back into it would duplicate them. Pipelines that
#' compile to a subquery over `table_name` (grouped filters, `mutate()`
#' followed by `filter()`) are also refused rather than mistranslated. Use
#' [show_ducklake_query()] to preview the generated SQL without running it.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("exec_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("exec_lake", lake_path = lake_dir)
#' create_table(data.frame(id = 1:3, status = "pending"), "jobs")
#'
#' # Update specific rows (table name inferred)
#' get_ducklake_table("jobs") |>
#'   dplyr::filter(id == 1) |>
#'   dplyr::mutate(status = "updated") |>
#'   ducklake_exec()
#'
#' # Delete rows matching a filter
#' get_ducklake_table("jobs") |>
#'   dplyr::filter(status == "pending") |>
#'   ducklake_exec()
#'
#' # Or provide the table name explicitly
#' get_ducklake_table("jobs") |>
#'   dplyr::mutate(status = "done") |>
#'   ducklake_exec("jobs")
#'
#' detach_ducklake("exec_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
ducklake_exec <- function(.data, table_name = NULL, .quiet = TRUE) {

  # Extract table name from attribute if not provided
  if (is.null(table_name)) {
    table_name <- attr(.data, "ducklake_table_name", exact = TRUE)
    if (is.null(table_name)) {
      cli::cli_abort("{.arg table_name} must be provided either as an argument or via {.fn get_ducklake_table}.")
    }
  }

  if (!.quiet) {
    cli::cli_text("Original dplyr SQL:")
    cli::cli_verbatim(as.character(dbplyr::remote_query(.data)))
  }

  # Generate (but do not run) the DuckLake SQL; it is executed once below
  sql_string <- update_table(.data, table_name, .quiet = TRUE, .execute = FALSE)

  if (!.quiet) {
    cli::cli_text("Translated DuckLake SQL:")
    cli::cli_verbatim(sql_string)
  }

  # Execute and return result
  result <- db_execute(sql_string)

  if (!.quiet) {
    cli::cli_text("Rows affected: {result}")
  }

  return(result)
}
