#' Replace a table with modified data and create a new snapshot
#'
#' @param .data A dplyr query object (tbl_lazy) with transformations
#' @param table_name Table name to replace
#' @param .quiet Logical, whether to suppress messages (default TRUE)
#'
#' @returns Invisibly returns NULL
#' @family table operations
#' @export
#'
#' @details
#' This function is designed for schema changes or bulk transformations that should
#' create a new versioned snapshot. It:
#' 1. Collects the transformed data
#' 2. Drops the existing table  
#' 3. Creates a new table with the updated schema/data
#' 
#' All operations happen within the current transaction context. Use
#' `begin_transaction()` and `commit_transaction()` to ensure proper versioning.
#' 
#' **When to use replace_table():**
#' - **Adding new columns** - DuckLake UPDATE cannot add columns; use replace_table()
#' - **Removing columns** - Restructure schema with select()
#' - **Complex transformations** - Apply full dplyr pipelines naturally
#'
#' **When to use [ducklake_exec()] instead:**
#' - Modifying existing column values only (no schema changes)
#' - Making targeted corrections to specific rows without rewriting the table
#'
#' Both paths create a snapshot: replace_table() via DROP + CREATE, and
#' ducklake_exec() via the in-place UPDATE/DELETE it runs, so either way the
#' change is available for time travel.
#' 
#' @examples
#' \dontrun{
#' # Add new derived columns with versioning
#' begin_transaction()
#' get_ducklake_table("adsl") |>
#'   mutate(
#'     AGE65FL = if_else(AGE >= 65, "Y", "N"),
#'     AGECAT = case_when(
#'       AGE < 65 ~ "<65",
#'       AGE >= 65 & AGE < 75 ~ "65-74",
#'       AGE >= 75 ~ ">=75"
#'     )
#'   ) |>
#'   replace_table("adsl")
#' commit_transaction()
#' 
#' # Remove columns and create new snapshot
#' begin_transaction()
#' get_ducklake_table("adsl") |>
#'   select(-AGE65FL, -AGECAT) |>
#'   replace_table("adsl")
#' commit_transaction()
#' }
replace_table <- function(.data, table_name, .quiet = TRUE) {

  if (!.quiet) {
    cli::cli_inform("Replacing table {.val {table_name}}...")
  }

  # Collect the transformed data
  new_data <- dplyr::collect(.data)

  if (!.quiet) {
    cli::cli_inform(
      "Collected {nrow(new_data)} row{?s} with {ncol(new_data)} column{?s}."
    )
  }

  # The drop and create must land together: outside a transaction they
  # autocommit separately, so a failed create would leave the table gone.
  # When the caller already opened a transaction, they own the
  # commit/rollback decision.
  conn <- get_ducklake_connection()
  own_txn <- !in_transaction(conn)
  committed <- FALSE
  if (own_txn) {
    DBI::dbExecute(conn, "BEGIN TRANSACTION;")
    on.exit(
      if (!committed) {
        tryCatch(DBI::dbExecute(conn, "ROLLBACK;"), error = function(e) NULL)
      },
      add = TRUE
    )
  }

  # Drop the existing table
  drop_sql <- sprintf("DROP TABLE IF EXISTS %s", quote_ident(table_name))
  db_execute(drop_sql)

  # Create the new table
  create_table(new_data, table_name)

  if (own_txn) {
    DBI::dbExecute(conn, "COMMIT;")
    committed <- TRUE
  }

  if (!.quiet) {
    cli::cli_inform("Table {.val {table_name}} successfully replaced.")
  }

  invisible(NULL)
}
