#' Upsert data from a dplyr query into a DuckLake table
#'
#' Performs a MERGE operation: updates existing rows and inserts new ones based on matching keys.
#' This is the pipeline-based version of rows_upsert() for use with dplyr queries.
#'
#' @param .data A dplyr query object (tbl_lazy) with the source data
#' @param table_name The target table name. If not provided, will be extracted from the table attribute (set by get_ducklake_table())
#' @param by Character vector of column names to match on (merge keys)
#' @param .quiet Logical, whether to suppress debug output (default TRUE)
#'
#' @return The result from executing the MERGE statement
#' @export
#'
#' @seealso [rows_upsert()] for the data.frame approach to upserts
#'
#' @details
#' This is the pipeline-based approach to upserts, ideal when transforming data with dplyr verbs.
#' For upserting data.frames directly, see [rows_upsert()].
#' 
#' This function generates a DuckDB INSERT ... ON CONFLICT statement (which provides MERGE/UPSERT functionality).
#' Rows are matched based on the columns specified in `by`. If a match is found, the row is updated;
#' if not, a new row is inserted.
#' 
#' **Note:** This function requires that the table has a PRIMARY KEY or UNIQUE constraint on the columns
#' specified in `by`. If your table doesn't have these constraints, use [rows_upsert()] instead.
#'
#' @examples
#' \dontrun{
#' # Upsert data from a computed query
#' get_ducklake_table("staging_table") |>
#'   mutate(processed = TRUE) |>
#'   upsert_table("target_table", by = "id")
#'
#' # Upsert with table name inferred
#' get_ducklake_table("my_table") |>
#'   filter(status == "active") |>
#'   mutate(last_updated = Sys.time()) |>
#'   upsert_table(by = c("id", "version"))
#' }
upsert_table <- function(.data, table_name = NULL, by, .quiet = TRUE) {
  
  # Extract table name from attribute if not provided
  if (is.null(table_name)) {
    table_name <- attr(.data, "ducklake_table_name", exact = TRUE)
    if (is.null(table_name)) {
      stop("table_name must be provided either as an argument or via get_ducklake_table()")
    }
  }
  
  if (missing(by) || is.null(by) || length(by) == 0) {
    stop("'by' parameter is required for upsert operations - specify the column(s) to match on")
  }
  
  if (!.quiet) {
    cat("\n=== Original dplyr SQL ===\n")
    print(dplyr::show_query(.data))
  }
  
  # Get the SQL from the dplyr query
  temp_file <- tempfile()
  sink(temp_file)
  dplyr::show_query(.data)
  sink()
  
  temp_sql <- readLines(temp_file)
  unlink(temp_file)
  
  if (length(temp_sql) == 0) {
    stop("No SQL content extracted from query")
  }
  
  combined_sql <- paste(temp_sql, collapse = " ")
  combined_sql <- gsub("<SQL>", "", combined_sql)
  combined_sql <- gsub("^\\s*", "", combined_sql)
  combined_sql <- gsub("\\s*$", "", combined_sql)
  combined_sql <- gsub("\\s+", " ", combined_sql)
  
  if (!.quiet) {
    cat("Cleaned source SQL:", combined_sql, "\n")
  }
  
  # Extract column names from the SELECT clause
  select_part <- gsub("SELECT\\s+(.+?)\\s+FROM.*", "\\1", combined_sql, perl = TRUE)
  columns <- strsplit(select_part, ",")[[1]]
  columns <- trimws(columns)
  
  # Get actual column names (handle AS clauses)
  col_names <- sapply(columns, function(col) {
    if (grepl("\\s+AS\\s+", col, ignore.case = TRUE)) {
      col_name <- gsub(".*\\s+AS\\s+([[:alnum:]_\"]+)$", "\\1", col, ignore.case = TRUE)
      gsub('"', '', col_name)
    } else if (grepl("\\*", col)) {
      "*"
    } else {
      # Simple column reference
      gsub('"', '', trimws(col))
    }
  }, USE.NAMES = FALSE)
  
  # Generate INSERT ... ON CONFLICT ... DO UPDATE statement
  # This is DuckDB's way of doing UPSERT/MERGE
  by_clause <- paste(by, collapse = ", ")
  
  # For the UPDATE SET clause, we need all columns except the key columns
  update_cols <- setdiff(col_names[col_names != "*"], by)
  
  if (length(update_cols) == 0) {
    stop("No columns to update - all columns are in the 'by' key set")
  }
  
  update_set <- paste(
    sapply(update_cols, function(col) {
      sprintf("%s = EXCLUDED.%s", col, col)
    }),
    collapse = ", "
  )
  
  # Build the MERGE statement using INSERT ... ON CONFLICT
  sql_string <- sprintf(
    "INSERT INTO %s %s ON CONFLICT (%s) DO UPDATE SET %s",
    table_name,
    combined_sql,
    by_clause,
    update_set
  )
  
  if (!.quiet) {
    cat("\n=== Generated MERGE/UPSERT SQL ===\n")
    cat(sql_string, "\n")
  }
  
  # Execute and return result
  result <- duckplyr::db_exec(sql_string)
  
  if (!.quiet) {
    cat("\nRows affected:", result, "\n")
  }
  
  return(result)
}
