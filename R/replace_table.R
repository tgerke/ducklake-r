#' Replace a table with modified data and create a new snapshot
#'
#' @param .data A dplyr query object (tbl_lazy) with transformations
#' @param table_name Table name to replace
#' @param .quiet Logical, whether to suppress messages (default TRUE)
#'
#' @return Invisibly returns NULL
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
#' - **Versioning needed** - Creates snapshots via DROP + CREATE for time travel
#' - **Complex transformations** - Apply full dplyr pipelines naturally
#' 
#' **When to use update_table() instead:**
#' - Modifying existing column values only (no schema changes)
#' - Performance critical and versioning not needed
#' - Making targeted corrections to specific rows
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
    cat("=== Replacing table:", table_name, "===\n")
  }
  
  # Collect the transformed data
  if (!.quiet) cat("Collecting transformed data...\n")
  new_data <- dplyr::collect(.data)
  
  if (!.quiet) {
    cat("Collected", nrow(new_data), "rows with", ncol(new_data), "columns\n")
  }
  
  # Drop the existing table
  if (!.quiet) cat("Dropping existing table...\n")
  drop_sql <- sprintf("DROP TABLE IF EXISTS %s", table_name)
  duckplyr::db_exec(drop_sql)
  
  # Create the new table
  if (!.quiet) cat("Creating new table...\n")
  create_table(new_data, table_name)
  
  if (!.quiet) {
    cat("Table", table_name, "successfully replaced\n")
  }
  
  invisible(NULL)
}
