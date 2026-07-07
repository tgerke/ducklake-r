#' Update existing column values in a table (in-place, no versioning)
#'
#' @param .data A dplyr query object (tbl_lazy) with mutate() operations
#' @param table_name Table name to update
#' @param .quiet Logical, whether to suppress debug output (default FALSE for backward compatibility)
#'
#' @returns Invisibly returns the SQL statement string after executing it
#' @keywords internal
#' @noRd
#'
#' @details
#' This function performs in-place UPDATE operations on existing columns.
#' **Important limitations:**
#' 
#' - **Cannot add or remove columns** - Only modifies values in existing columns
#' - **Does not create snapshots** - UPDATE operations modify in-place without creating 
#'   snapshots, even when wrapped in transactions. Only CREATE operations trigger snapshots.
#' - **All columns must exist** - Any column referenced in mutate() must already exist in the table
#' 
#' Use `replace_table()` if you need to:
#' - Add new derived columns
#' - Remove columns
#' - Create a new versioned snapshot
#' 
#' Use `update_table()` when:
#' - Making targeted value corrections to existing columns
#' - Performance is critical and versioning is not needed
#' - Updating specific rows with filter()
#'
#' @examples
#' \dontrun{
#' # Correct a specific value (no versioning needed)
#' get_ducklake_table("adsl") |>
#'   mutate(SAFFL = if_else(USUBJID == "01-701-1015", "N", SAFFL)) |>
#'   update_table("adsl")
#' 
#' # Update multiple columns
#' get_ducklake_table("adae") |>
#'   mutate(
#'     AESEV = if_else(AESEV == "MILD", "MODERATE", AESEV),
#'     AESER = if_else(AESEV == "SEVERE", "Y", AESER)
#'   ) |>
#'   update_table("adae")
#' }
update_table <- function(.data, table_name, .quiet = FALSE) {

  if (!.quiet) {
    cli::cli_inform("Translating dplyr query into an in-place statement for {.val {table_name}}.")
  }

  tryCatch({
    # Render the dplyr query to SQL (no sink()/tempfile indirection)
    combined_sql <- as.character(dbplyr::remote_query(.data))
    combined_sql <- gsub("\\s+", " ", trimws(combined_sql))

    if (!nzchar(combined_sql)) {
      cli::cli_abort("No SQL could be rendered from {.arg .data}.")
    }

    if (!.quiet) cli::cli_inform("Rendered SQL: {.code {combined_sql}}")

    # This regex surgery only works on a simple single-SELECT query.
    # Refuse anything with subqueries or multiple WHERE clauses rather than
    # silently generating wrong SQL.
    n_where <- length(gregexpr("\\bWHERE\\b", combined_sql)[[1]])
    if (grepl("\\bWHERE\\b", combined_sql) && n_where > 1) {
      cli::cli_abort(c(
        "The dplyr query is too complex for an in-place update ({n_where} WHERE clauses found).",
        "i" = "Use {.fn replace_table} for complex transformations, or {.fn ducklake_exec} with explicit SQL."
      ))
    }
    if (grepl("\\(\\s*SELECT\\b", combined_sql, ignore.case = TRUE)) {
      cli::cli_abort(c(
        "The dplyr query contains a subquery, which in-place updates do not support.",
        "i" = "Use {.fn replace_table} for complex transformations, or {.fn ducklake_exec} with explicit SQL."
      ))
    }

    # Determine operation type
    has_where <- grepl("\\bWHERE\\b", combined_sql)
    has_case_when <- grepl("CASE WHEN", combined_sql)
    has_select_star <- grepl("SELECT\\s+[^,]*\\*", combined_sql)

    # Operation detection
    if (has_where && !has_case_when) {
      operation_type <- "delete"
    } else if (has_case_when) {
      operation_type <- "update"
    } else {
      operation_type <- "insert"
    }

    if (!.quiet) cli::cli_inform("Operation type: {.val {operation_type}}")

    quoted_table <- quote_ident(table_name)

    # Generate SQL
    if (operation_type == "delete") {
      where_part <- sub("^.*?\\bWHERE\\b\\s+", "", combined_sql)
      result_sql <- sprintf("DELETE FROM %s WHERE NOT (%s)", quoted_table, where_part)
    } else if (operation_type == "update") {
      assignments <- extract_assignments_from_sql(combined_sql)
      result_sql <- sprintf("UPDATE %s SET %s", quoted_table, assignments)

      if (has_where) {
        where_part <- sub("^.*?\\bWHERE\\b\\s+", "", combined_sql)
        result_sql <- paste(result_sql, "WHERE", where_part)
      }
    } else {
      result_sql <- sprintf("INSERT INTO %s %s", quoted_table, combined_sql)
    }

    if (!.quiet) cli::cli_inform("Generated SQL: {.code {result_sql}}")

    # Execute the generated SQL directly
    db_execute(result_sql)

    # Return invisibly for potential chaining
    invisible(result_sql)

  }, error = function(e) {
    cli::cli_abort("Failed to generate DuckLake SQL: {e$message}")
  })
}

#' Extract column assignments from SQL SELECT statement
#'
#' @param sql_text A SQL SELECT statement
#' @returns A string of comma-separated column assignments for UPDATE SET clause
#' @keywords internal
extract_assignments_from_sql <- function(sql_text) {
  select_part <- gsub("SELECT\\s+(.+?)\\s+FROM.*", "\\1", sql_text, perl = TRUE)

  columns <- strsplit(select_part, ",")[[1]]
  columns <- trimws(columns)

  assignments <- c()

  for (col in columns) {
    col <- trimws(col)

    if (grepl("CASE WHEN.*END AS", col)) {
      # Extract column name and CASE expression
      col_name <- gsub(".*AS\\s+([[:alnum:]_\"]+)$", "\\1", col)
      col_name <- gsub('"', '', col_name)  # Remove quotes
      case_expr <- gsub("(.+?)\\s+AS\\s+[[:alnum:]_\"]+$", "\\1", col)
      assignments <- c(assignments, paste0(col_name, " = ", case_expr))

    } else if (grepl("AS\\s+([[:alnum:]_\"]+)$", col)) {
      # Regular expression with AS
      col_name <- gsub(".*AS\\s+([[:alnum:]_\"]+)$", "\\1", col)
      col_name <- gsub('"', '', col_name)
      expr <- gsub("(.+?)\\s+AS\\s+[[:alnum:]_\"]+$", "\\1", col)
      assignments <- c(assignments, paste0(col_name, " = ", expr))
    }
    # Skip simple column references (they don't need assignments)
  }

  if (length(assignments) == 0) {
    cli::cli_abort("No column assignments found for UPDATE operation.")
  }

  paste(assignments, collapse = ", ")
}
