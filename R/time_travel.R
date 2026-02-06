#' Query a table at a specific timestamp (time travel)
#'
#' Retrieves data from a DuckLake table as it existed at a specific point in time
#' using DuckLake's AT (TIMESTAMP => ...) syntax.
#'
#' @param table_name The name of the table to query
#' @param timestamp A POSIXct timestamp or character string in ISO 8601 format (e.g., "2024-01-15 10:30:00")
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @return A dplyr lazy query object (tbl_lazy) that can be further manipulated with dplyr verbs
#' @export
#'
#' @details
#' DuckLake supports time-travel queries, allowing you to query historical data
#' as it existed at a specific timestamp. This uses the syntax:
#' \code{SELECT * FROM table AT (TIMESTAMP => 'timestamp')}
#' 
#' This is useful for:
#' - Auditing changes over time
#' - Recovering accidentally deleted or modified data  
#' - Comparing data states across different time points
#' - Regulatory compliance and data lineage documentation
#'
#' The timestamp must be within the range of available snapshots for the table.
#' Use \code{list_table_snapshots()} to see available snapshot times.
#'
#' @examples
#' \dontrun{
#' # Query data as it existed yesterday
#' yesterday <- Sys.time() - (24 * 60 * 60)
#' get_ducklake_table_asof("my_table", yesterday) |>
#'   filter(category == "A") |>
#'   collect()
#'
#' # Query data at a specific snapshot time
#' snapshots <- list_table_snapshots("my_table")
#' get_ducklake_table_asof("my_table", snapshots$snapshot_time[2]) |>
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
  
  # Add schema prefix if not already present
  if (!grepl("\\.", table_name)) {
    table_name <- paste0("main.", table_name)
  }
  
  # Use DuckLake's AT (TIMESTAMP => ...) syntax for time travel
  query <- sprintf("SELECT * FROM %s AT (TIMESTAMP => '%s')", 
                   table_name, timestamp_str)
  
  # Return as a dplyr tbl
  result <- dplyr::tbl(conn, dplyr::sql(query))
  
  # Store the table name as an attribute for potential use with ducklake_exec()
  attr(result, "ducklake_table_name") <- table_name
  
  return(result)
}

#' Query a table at a specific version/snapshot
#'
#' Retrieves data from a DuckLake table at a specific snapshot ID using DuckLake's
#' AT (VERSION => ...) syntax.
#'
#' @param table_name The name of the table to query
#' @param version The snapshot_id to query (get this from \code{list_table_snapshots()})
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @return A dplyr lazy query object (tbl_lazy) that can be further manipulated with dplyr verbs
#' @export
#'
#' @details
#' This function allows you to query a specific snapshot of a table using its snapshot_id.
#' This uses the syntax: \code{SELECT * FROM table AT (VERSION => snapshot_id)}
#' 
#' Each time you create or modify a table within a transaction, DuckLake creates a new
#' snapshot with a unique snapshot_id. Note that snapshot_id and schema_version are 
#' typically the same value - both represent the snapshot identifier.
#'
#' Use \code{list_table_snapshots(table_name)} to see all available snapshots and their IDs.
#'
#' @examples
#' \dontrun{
#' # Get available snapshots
#' snapshots <- list_table_snapshots("my_table")
#' 
#' # Query the first snapshot version
#' get_ducklake_table_version("my_table", snapshots$snapshot_id[1]) |>
#'   filter(status == "active") |>
#'   collect()
#' }
get_ducklake_table_version <- function(table_name, version, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  # Get current ducklake name
  tryCatch({
    current_db <- DBI::dbGetQuery(conn, "SELECT current_database() as db")$db
    ducklake_name <- current_db
  }, error = function(e) {
    stop("Could not determine ducklake_name. Make sure a ducklake is attached.")
  })
  
  # Add schema prefix if not already present
  if (!grepl("\\.", table_name)) {
    table_name <- paste0("main.", table_name)
  }
  
  # Use DuckLake's AT (VERSION => ...) syntax to query a specific snapshot
  # The version parameter should be the snapshot_id from list_table_snapshots()
  query <- sprintf("SELECT * FROM %s AT (VERSION => %d)", 
                   table_name, version)
  
  dplyr::tbl(conn, dplyr::sql(query))
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
list_table_snapshots <- function(table_name = NULL, ducklake_name = NULL, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  # If ducklake_name not provided, try to infer from current database
  if (is.null(ducklake_name)) {
    tryCatch({
      current_db <- DBI::dbGetQuery(conn, "SELECT current_database() as db")$db
      if (!is.null(current_db) && current_db != "") {
        ducklake_name <- current_db
      }
    }, error = function(e) {
      stop("Could not determine ducklake_name. Please provide it explicitly.")
    })
  }
  
  # Query snapshots using the DuckLake snapshots() function
  tryCatch({
    query <- sprintf("SELECT * FROM %s.snapshots()", ducklake_name)
    result <- DBI::dbGetQuery(conn, query)
    
    # If table_name is provided, filter the results
    if (!is.null(table_name) && nrow(result) > 0) {
      # Filter by checking if table_name appears in changes column
      # The changes column contains comma-separated values like "tables_created, tables_inserted_into, main.dm_raw, 1"
      # We need to match the full table name including schema prefix, with word boundaries
      full_table_name <- paste0("main.", table_name)
      # Use regex with word boundaries to avoid matching "main.dm" when looking for "main.dm_raw"
      pattern <- paste0("\\b", gsub("\\.", "\\\\.", full_table_name), "\\b")
      result <- result[grepl(pattern, result$changes), ]
    }
    
    return(result)
  }, error = function(e) {
    warning("Could not retrieve snapshot information. ",
            "Make sure the ducklake is attached and has snapshots. ",
            "Error: ", e$message)
    return(data.frame())
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
