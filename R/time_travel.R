#' Query a table at a specific timestamp (time travel)
#'
#' Retrieves data from a DuckLake table as it existed at a specific point in time
#' using DuckLake's AT (TIMESTAMP => ...) syntax.
#'
#' @param table_name The name of the table to query
#' @param timestamp A POSIXct timestamp (converted to UTC, which is how
#'   DuckLake records snapshot times) or character string in ISO 8601 format
#'   already in UTC (e.g., "2024-01-15 10:30:00")
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @returns A dplyr lazy query object (tbl_lazy) that can be further manipulated with dplyr verbs
#' @family time travel
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
#' **Important**: When querying at a snapshot's exact timestamp, you may need to 
#' add a small time buffer (e.g., +1 second) to ensure the snapshot is found.
#' This is because the time-travel query looks for snapshots created at or before
#' the specified timestamp.
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
#' # Add 1 second to ensure the snapshot is found
#' get_ducklake_table_asof("my_table", snapshots$snapshot_time[2] + 1) |>
#'   summarise(total = sum(amount))
#' }
get_ducklake_table_asof <- function(table_name, timestamp, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  # DuckLake reads naive timestamp literals as UTC, so POSIXct values are
  # rendered in UTC; character input must already be UTC
  timestamp_str <- format_timestamp(timestamp)

  # Add schema prefix if not already present.
  # DuckDB and SQLite use the main. schema; PostgreSQL and MySQL do not.
  if (!grepl("\\.", table_name)) {
    backend <- get_ducklake_backend()
    if (!(backend %in% c("postgres", "mysql"))) {
      table_name <- paste0("main.", table_name)
    }
  }
  
  # Use DuckLake's AT (TIMESTAMP => ...) syntax for time travel
  query <- sprintf("SELECT * FROM %s AT (TIMESTAMP => %s)",
                   quote_ident(table_name, conn), quote_sql(timestamp_str))
  
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
#' @returns A dplyr lazy query object (tbl_lazy) that can be further manipulated with dplyr verbs
#' @family time travel
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

  # Add schema prefix if not already present.
  # DuckDB and SQLite use the main. schema; PostgreSQL and MySQL do not.
  if (!grepl("\\.", table_name)) {
    backend <- get_ducklake_backend()
    if (!(backend %in% c("postgres", "mysql"))) {
      table_name <- paste0("main.", table_name)
    }
  }

  # Use DuckLake's AT (VERSION => ...) syntax to query a specific snapshot
  # The version parameter should be the snapshot_id from list_table_snapshots()
  query <- sprintf("SELECT * FROM %s AT (VERSION => %d)",
                   quote_ident(table_name, conn), as.integer(version))

  dplyr::tbl(conn, dplyr::sql(query))
}

#' List available snapshots for a table
#'
#' Retrieves information about available snapshots/versions for a table.
#'
#' @param table_name The name of the table to query
#' @param ducklake_name The name of the ducklake (database) to query. If NULL, will attempt to infer from current database.
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @returns A data frame with snapshot information (version, timestamp, etc.)
#' @family time travel
#' @export
#'
#' @details
#' This function queries the snapshot history of a table, showing available
#' versions and their timestamps. This is useful for understanding what
#' historical versions are available for time-travel queries.
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
      cli::cli_abort("Could not determine {.arg ducklake_name}. Please provide it explicitly.")
    })
  }
  
  # Query snapshots using the DuckLake snapshots() function
  tryCatch({
    query <- sprintf("SELECT * FROM %s.snapshots()", quote_ident(ducklake_name, conn))
    result <- DBI::dbGetQuery(conn, query)
    
    # If table_name is provided, filter the results
    if (!is.null(table_name) && nrow(result) > 0) {
      # Filter by checking if the table appears in the changes column.
      # Snapshots that create a table reference it by name
      # ("tables_created, tables_inserted_into, main.dm_raw, 1"), but
      # row-level DML snapshots reference it by numeric table id only
      # ("inlined_insert, 1"), so match on both.
      # DuckDB and SQLite use main. schema prefix; PostgreSQL and MySQL do not
      backend <- get_ducklake_backend()
      if (backend %in% c("postgres", "mysql")) {
        full_table_name <- table_name
      } else {
        full_table_name <- paste0("main.", table_name)
      }
      # Use regex with word boundaries to avoid matching "main.dm" when looking for "main.dm_raw"
      patterns <- paste0("\\b", gsub("\\.", "\\\\.", full_table_name), "\\b")

      # Resolve every table id this name has had (replace_table assigns a
      # new id each time), so id-only DML snapshots are matched too
      table_ids <- tryCatch({
        metadata_ref <- if (backend %in% c("postgres", "mysql")) {
          paste0("__ducklake_metadata_", ducklake_name, ".ducklake_table")
        } else {
          paste0("__ducklake_metadata_", ducklake_name, ".main.ducklake_table")
        }
        DBI::dbGetQuery(
          conn,
          sprintf(
            "SELECT DISTINCT table_id FROM %s WHERE table_name = ?",
            quote_ident(metadata_ref, conn)
          ),
          params = list(table_name)
        )$table_id
      }, error = function(e) integer(0))
      if (length(table_ids) > 0) {
        patterns <- c(patterns, paste0("\\b", table_ids, "\\b"))
      }

      keep <- Reduce(`|`, lapply(patterns, grepl, x = result$changes))
      result <- result[keep, ]
      rownames(result) <- NULL
    }

    return(result)
  }, error = function(e) {
    cli::cli_warn(c(
      "Could not retrieve snapshot information.",
      "i" = "Make sure the ducklake is attached and has snapshots.",
      "x" = e$message
    ))
    return(data.frame())
  })
}

#' Restore a table to a previous version
#'
#' Rolls a table back to the state it had at an earlier snapshot or point in
#' time, by recreating it from a time-travel read of itself. History is
#' preserved: the restore is recorded as a **new** snapshot (with a commit
#' message noting the restore), so nothing is rewritten or lost and you can
#' still time-travel to any snapshot, including those after the restore point.
#'
#' @param table_name The name of the table to restore
#' @param version Optional snapshot id to restore to (see [list_table_snapshots()])
#' @param timestamp Optional timestamp to restore to (POSIXct, converted to
#'   UTC, or character already in UTC)
#' @param author Optional author to record on the restore snapshot, for the
#'   audit trail
#' @param commit_message Optional commit message for the restore snapshot.
#'   Defaults to a message noting the restore point (e.g.
#'   `"Restored my_table to snapshot 5"`).
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @returns Invisibly returns TRUE on success
#' @family time travel
#' @export
#'
#' @details
#' You must specify either \code{version} or \code{timestamp}, but not both.
#'
#' Under the hood this runs
#' \code{CREATE OR REPLACE TABLE t AS SELECT * FROM t AT (VERSION => n)}
#' inside a transaction. Because the restore creates a new snapshot, it is
#' itself reversible with another \code{restore_table_version()} call.
#'
#' @seealso [get_ducklake_table_version()], [get_ducklake_table_asof()],
#'   [list_table_snapshots()]
#'
#' @examples
#' \dontrun{
#' # Restore to snapshot 5
#' restore_table_version("my_table", version = 5)
#'
#' # Restore to a specific timestamp
#' restore_table_version("my_table", timestamp = "2024-01-15 10:00:00")
#'
#' # Record who performed the restore in the audit trail
#' restore_table_version(
#'   "my_table",
#'   version = 5,
#'   author = "Data Steward",
#'   commit_message = "Roll back erroneous bulk update"
#' )
#' }
restore_table_version <- function(table_name, version = NULL, timestamp = NULL,
                                  author = NULL, commit_message = NULL,
                                  conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }

  # Check that exactly one of version or timestamp is provided
  if (is.null(version) && is.null(timestamp)) {
    cli::cli_abort("Must provide either {.arg version} or {.arg timestamp}.")
  }
  if (!is.null(version) && !is.null(timestamp)) {
    cli::cli_abort("Cannot provide both {.arg version} and {.arg timestamp}.")
  }

  if (!is.null(version)) {
    at_clause <- sprintf("VERSION => %d", as.integer(version))
    restore_point <- sprintf("snapshot %d", as.integer(version))
  } else {
    # DuckLake reads naive timestamp literals as UTC, so POSIXct values are
    # rendered in UTC; character input must already be UTC
    timestamp_str <- format_timestamp(timestamp)
    at_clause <- sprintf("TIMESTAMP => %s", quote_sql(timestamp_str))
    restore_point <- timestamp_str
  }

  quoted_table <- quote_ident(table_name, conn)
  restore_sql <- sprintf(
    "CREATE OR REPLACE TABLE %s AS SELECT * FROM %s AT (%s)",
    quoted_table, quoted_table, at_clause
  )

  if (is.null(commit_message)) {
    commit_message <- sprintf("Restored %s to %s", table_name, restore_point)
  }

  tryCatch({
    with_transaction(
      db_execute(restore_sql, conn = conn),
      author = author,
      commit_message = commit_message,
      conn = conn
    )
    cli::cli_inform("Table {.val {table_name}} restored to {restore_point} (recorded as a new snapshot).")
    invisible(TRUE)
  }, error = function(e) {
    cli::cli_abort(c(
      "Failed to restore table {.val {table_name}}.",
      "x" = e$message,
      "i" = "Check {.code list_table_snapshots(\"{table_name}\")} for available snapshots."
    ))
  })
}
