#' Update existing column values in a table (in-place, no versioning)
#'
#' @param .data A dplyr query object (tbl_lazy) with mutate() operations
#' @param table_name Table name to update
#' @param .quiet Logical, whether to suppress debug output (default FALSE for backward compatibility)
#'
#' @return Invisibly returns the SQL statement string after executing it
#' @export
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
#' \dontrun{n#' # Correct a specific value (no versioning needed)
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
    cat("=== DEBUG: update_table called ===\n")
    cat("Query class:", class(.data), "\n")
    cat("Target table:", table_name, "\n")
  }

  tryCatch({
    if (!.quiet) cat("=== DEBUG: Getting SQL directly ===\n")

    # Get the SQL
    temp_file <- tempfile()
    sink(temp_file)
    dplyr::show_query(.data)
    sink()

    temp_sql <- readLines(temp_file)
    unlink(temp_file)

    if (!.quiet) cat("Raw SQL lines:", length(temp_sql), "\n")

    if (length(temp_sql) == 0) {
      stop("No SQL content extracted")
    }

    combined_sql <- paste(temp_sql, collapse = " ")
    combined_sql <- gsub("<SQL>", "", combined_sql)
    combined_sql <- gsub("^\\s*", "", combined_sql)
    combined_sql <- gsub("\\s*$", "", combined_sql)
    combined_sql <- gsub("\\s+", " ", combined_sql)

    if (!.quiet) cat("Cleaned SQL:", combined_sql, "\n")

    # Determine operation type
    has_where <- grepl("WHERE", combined_sql)
    has_case_when <- grepl("CASE WHEN", combined_sql)
    has_select_star <- grepl("SELECT\\s+[^,]*\\*", combined_sql)

    if (!.quiet) {
      cat("SQL analysis - has_where:", has_where, "has_case_when:", has_case_when,
          "has_select_star:", has_select_star, "\n")
    }

    # Operation detection
    if (has_where && has_select_star) {
      operation_type <- "delete"
    } else if (has_case_when) {
      operation_type <- "update"
    } else if (has_where && !has_select_star) {
      operation_type <- "delete"
    } else {
      operation_type <- "insert"
    }

    if (!.quiet) cat("Operation type:", operation_type, "\n")

    # Generate SQL
    if (operation_type == "delete") {
      where_part <- gsub(".*WHERE\\s+(.+)", "\\1", combined_sql)
      if (!.quiet) cat("Extracted WHERE part:", where_part, "\n")

      result_sql <- sprintf("DELETE FROM %s WHERE NOT (%s)", table_name, where_part)
    } else if (operation_type == "update") {
      assignments <- extract_assignments_from_sql(combined_sql)
      if (!.quiet) cat("Extracted assignments:", assignments, "\n")

      result_sql <- sprintf("UPDATE %s SET %s", table_name, assignments)

      if (has_where) {
        where_part <- gsub(".*WHERE\\s+(.+)", "\\1", combined_sql)
        result_sql <- paste(result_sql, "WHERE", where_part)
      }
    } else {
      result_sql <- sprintf("INSERT INTO %s %s", table_name, combined_sql)
    }

    if (!.quiet) cat("Generated SQL:", result_sql, "\n")
    
    # Execute the generated SQL directly
    result <- duckplyr::db_exec(result_sql)
    
    # Return invisibly for potential chaining
    invisible(result_sql)

  }, error = function(e) {
    stop("Failed to generate DuckLake SQL: ", e$message)
  })
}

#' Extract column assignments from SQL SELECT statement
#'
#' @param sql_text A SQL SELECT statement
#' @return A string of comma-separated column assignments for UPDATE SET clause
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
    stop("No column assignments found for UPDATE operation")
  }

  paste(assignments, collapse = ", ")
}
