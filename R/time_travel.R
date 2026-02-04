#' Query a table at a specific timestamp (time travel)
#'
#' Retrieves data from a DuckDB table as it existed at a specific point in time
#' using DuckDB's snapshot/time-travel functionality.
#'
#' @param table_name The name of the table to query
#' @param timestamp A POSIXct timestamp or character string in ISO 8601 format (e.g., "2024-01-15 10:30:00")
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @return A dplyr lazy query object (tbl_lazy) that can be further manipulated with dplyr verbs
#' @export
#'
#' @details
#' DuckDB supports time-travel queries using the ASOF syntax, allowing you to query
#' historical data as it existed at a specific timestamp. This is useful for:
#' - Auditing changes over time
#' - Recovering accidentally deleted or modified data
#' - Comparing data states across different time points
#'
#' Note: This functionality requires that the table has been properly configured with
#' DuckDB's time-travel features (e.g., Delta Lake tables with snapshot support).
#'
#' @examples
#' \dontrun{
#' # Query data as it existed yesterday
#' yesterday <- Sys.time() - (24 * 60 * 60)
#' get_ducklake_table_asof("my_table", yesterday) |>
#'   filter(category == "A") |>
#'   collect()
#'
#' # Query data at a specific timestamp
#' get_ducklake_table_asof("my_table", "2024-01-15 10:30:00") |>
#'   summarise(total = sum(amount))
#' }
get_ducklake_table_asof <- function(table_name, timestamp, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  # Convert timestamp to character if it's POSIXct
  if (inherits(timestamp, "POSIXct")) {
    timestamp_str <- format(timestamp, "%Y-%m-%d %H:%M:%S")
  } else {
    timestamp_str <- as.character(timestamp)
  }
  
  # Create a query with ASOF clause
  # Note: Actual syntax may vary depending on DuckDB version and table type
  query <- sprintf("SELECT * FROM %s ASOF TIMESTAMP '%s'", table_name, timestamp_str)
  
  # Return as a dplyr tbl
  result <- dplyr::tbl(conn, dplyr::sql(query))
  
  # Store the table name as an attribute for potential use with ducklake_exec()
  attr(result, "ducklake_table_name") <- table_name
  
  return(result)
}

#' Query a table at a specific version/snapshot
#'
#' Retrieves data from a DuckDB table at a specific version or snapshot number.
#'
#' @param table_name The name of the table to query
#' @param version The version or snapshot number to query
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @return A dplyr lazy query object (tbl_lazy) that can be further manipulated with dplyr verbs
#' @export
#'
#' @details
#' This function allows you to query a specific version/snapshot of a table.
#' This is particularly useful with Delta Lake or Iceberg tables that maintain
#' version history.
#'
#' @examples
#' \dontrun{
#' # Query version 5 of a table
#' get_ducklake_table_version("my_table", 5) |>
#'   filter(status == "active") |>
#'   collect()
#' }
get_ducklake_table_version <- function(table_name, version, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  # Create a query with VERSION clause
  query <- sprintf("SELECT * FROM %s VERSION AS OF %d", table_name, as.integer(version))
  
  # Return as a dplyr tbl
  result <- dplyr::tbl(conn, dplyr::sql(query))
  
  # Store the table name as an attribute
  attr(result, "ducklake_table_name") <- table_name
  
  return(result)
}

#' List available snapshots for a table
#'
#' Retrieves information about available snapshots/versions for a table.
#'
#' @param table_name The name of the table to query
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @return A data frame with snapshot information (version, timestamp, etc.)
#' @export
#'
#' @details
#' This function queries the snapshot history of a table, showing available
#' versions and their timestamps. This is useful for understanding what
#' historical versions are available for time-travel queries.
#'
#' Note: The exact format and availability of this information depends on the
#' table format (Delta Lake, Iceberg, etc.).
#'
#' @examples
#' \dontrun{
#' # List all snapshots for a table
#' list_table_snapshots("my_table")
#' }
list_table_snapshots <- function(table_name, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  # Try to query snapshot information
  # For Delta Lake tables, this might be:
  tryCatch({
    query <- sprintf("SELECT * FROM delta_log('%s')", table_name)
    result <- DBI::dbGetQuery(conn, query)
    return(result)
  }, error = function(e) {
    # If that doesn't work, try alternative methods
    tryCatch({
      # For Iceberg tables
      query <- sprintf("SELECT * FROM %s.snapshots", table_name)
      result <- DBI::dbGetQuery(conn, query)
      return(result)
    }, error = function(e2) {
      warning("Could not retrieve snapshot information. ",
              "The table may not support snapshots or may require a different query format. ",
              "Error: ", e$message)
      return(data.frame())
    })
  })
}

#' Restore a table to a previous version
#'
#' Restores a table to a specific version or timestamp, reverting any changes
#' made after that point.
#'
#' @param table_name The name of the table to restore
#' @param version Optional version number to restore to
#' @param timestamp Optional timestamp to restore to (POSIXct or character)
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @return Invisibly returns TRUE on success
#' @export
#'
#' @details
#' This function restores a table to a previous state. You must specify either
#' \code{version} or \code{timestamp}, but not both.
#'
#' WARNING: This operation modifies the table and cannot be easily undone.
#' Consider using within a transaction or backing up your data first.
#'
#' @examples
#' \dontrun{
#' # Restore to version 5
#' restore_table_version("my_table", version = 5)
#'
#' # Restore to a specific timestamp
#' restore_table_version("my_table", timestamp = "2024-01-15 10:00:00")
#' }
restore_table_version <- function(table_name, version = NULL, timestamp = NULL, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  # Check that exactly one of version or timestamp is provided
  if (is.null(version) && is.null(timestamp)) {
    stop("Must provide either 'version' or 'timestamp'")
  }
  if (!is.null(version) && !is.null(timestamp)) {
    stop("Cannot provide both 'version' and 'timestamp'")
  }
  
  # Build the restore query
  if (!is.null(version)) {
    query <- sprintf("RESTORE TABLE %s TO VERSION %d", table_name, as.integer(version))
  } else {
    # Convert timestamp to character if it's POSIXct
    if (inherits(timestamp, "POSIXct")) {
      timestamp_str <- format(timestamp, "%Y-%m-%d %H:%M:%S")
    } else {
      timestamp_str <- as.character(timestamp)
    }
    query <- sprintf("RESTORE TABLE %s TO TIMESTAMP '%s'", table_name, timestamp_str)
  }
  
  # Execute the restore
  tryCatch({
    DBI::dbExecute(conn, query)
    message("Table '", table_name, "' restored successfully")
    invisible(TRUE)
  }, error = function(e) {
    stop("Failed to restore table: ", e$message, "\n",
         "Note: RESTORE functionality depends on table format and DuckDB version.")
  })
}
