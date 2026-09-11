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
#' @returns A lazy table (class `tbl_ducklake`) that works with dplyr verbs.
#'   Like [get_ducklake_table()], collecting it restores stored column
#'   labels.
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
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("asof_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("asof_lake", lake_path = lake_dir)
#' create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "orders")
#'
#' rows_insert(
#'   get_ducklake_table("orders"),
#'   data.frame(id = 4L, amount = 40),
#'   by = "id"
#' )
#'
#' # Query data at a specific snapshot time
#' snapshots <- list_table_snapshots("orders")
#' # Add 1 second to ensure the snapshot is found
#' get_ducklake_table_asof("orders", snapshots$snapshot_time[1] + 1) |>
#'   dplyr::summarise(total = sum(amount)) |>
#'   dplyr::collect()
#'
#' detach_ducklake("asof_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
get_ducklake_table_asof <- function(table_name, timestamp, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  # DuckLake reads naive timestamp literals as UTC, so POSIXct values are
  # rendered in UTC; character input must already be UTC
  timestamp_str <- format_timestamp(timestamp)

  # Add schema prefix if not already present.
  # DuckDB and SQLite use the main. schema; PostgreSQL and MySQL do not.
  qualified <- table_name
  if (!grepl("\\.", table_name)) {
    backend <- get_ducklake_backend()
    if (!(backend %in% c("postgres", "mysql"))) {
      qualified <- paste0("main.", table_name)
    }
  }

  # Use DuckLake's AT (TIMESTAMP => ...) syntax for time travel
  query <- sprintf("SELECT * FROM %s AT (TIMESTAMP => %s)",
                   quote_ident(qualified, conn), quote_sql(timestamp_str))

  as_ducklake_tbl(dplyr::tbl(conn, dplyr::sql(query)), table_name)
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
#' @returns A lazy table (class `tbl_ducklake`) that works with dplyr verbs.
#'   Like [get_ducklake_table()], collecting it restores stored column
#'   labels.
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
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("version_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("version_lake", lake_path = lake_dir)
#' create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "orders")
#'
#' rows_insert(
#'   get_ducklake_table("orders"),
#'   data.frame(id = 4L, amount = 40),
#'   by = "id"
#' )
#'
#' # Get available snapshots
#' snapshots <- list_table_snapshots("orders")
#'
#' # Query the first snapshot version
#' get_ducklake_table_version("orders", snapshots$snapshot_id[1]) |>
#'   dplyr::collect()
#'
#' detach_ducklake("version_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
get_ducklake_table_version <- function(table_name, version, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }

  # Add schema prefix if not already present.
  # DuckDB and SQLite use the main. schema; PostgreSQL and MySQL do not.
  qualified <- table_name
  if (!grepl("\\.", table_name)) {
    backend <- get_ducklake_backend()
    if (!(backend %in% c("postgres", "mysql"))) {
      qualified <- paste0("main.", table_name)
    }
  }

  # Use DuckLake's AT (VERSION => ...) syntax to query a specific snapshot
  # The version parameter should be the snapshot_id from list_table_snapshots()
  query <- sprintf("SELECT * FROM %s AT (VERSION => %d)",
                   quote_ident(qualified, conn), as.integer(version))

  as_ducklake_tbl(dplyr::tbl(conn, dplyr::sql(query)), table_name)
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
#' A table's snapshots are matched by its name and by every table id the
#' name has had in its schema, so the history stays complete across
#' [replace_table()] and [restore_table_version()], which give the table a
#' new id.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("snaplist_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("snaplist_lake", lake_path = lake_dir)
#' create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "orders")
#'
#' # List all snapshots for a table
#' list_table_snapshots("orders")
#'
#' detach_ducklake("snaplist_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
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
    
    # If table_name is provided, keep the snapshots that touched it
    if (!is.null(table_name) && nrow(result) > 0) {
      keep <- snapshots_touching_table(
        result$changes, table_name, ducklake_name, conn
      )
      result <- result[keep, , drop = FALSE]
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
#'   audit trail. Defaults to the `ducklake.author` option when it is set.
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
#' Under the hood this reads \code{SELECT * FROM t AT (VERSION => n)} into a
#' temporary DuckDB table, then drops and recreates \code{t} from it inside
#' a transaction. Because the restore creates a new snapshot, it is itself
#' reversible with another \code{restore_table_version()} call.
#'
#' The restored table gets a new table id, and DuckLake keeps comments,
#' partition keys, sort order, and table-scoped options against the id, so
#' they are captured beforehand and put back: comments and keys for the
#' columns the restored version still has, inside the restore transaction;
#' table-scoped options right after it commits, as a small follow-up
#' snapshot, since DuckLake cannot set options on a table created in the
#' open transaction.
#'
#' @seealso [get_ducklake_table_version()], [get_ducklake_table_asof()],
#'   [list_table_snapshots()]
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("restore_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("restore_lake", lake_path = lake_dir)
#' create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "orders")
#'
#' rows_delete(
#'   get_ducklake_table("orders"),
#'   data.frame(id = 1L),
#'   by = "id"
#' )
#' snapshots <- list_table_snapshots("orders")
#' first_version <- snapshots$snapshot_id[1]
#'
#' # Roll the table back to its first snapshot
#' restore_table_version("orders", version = first_version)
#'
#' # Record who performed the restore in the audit trail
#' restore_table_version(
#'   "orders",
#'   version = first_version,
#'   author = "Data Steward",
#'   commit_message = "Roll back erroneous bulk update"
#' )
#'
#' detach_ducklake("restore_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
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

  if (is.null(commit_message)) {
    commit_message <- sprintf("Restored %s to %s", table_name, restore_point)
  }

  # The rewrite assigns a new table id; carry the comments, partition keys,
  # sort order, and table-scoped options over to it
  meta <- capture_table_metadata(table_name, conn = conn)

  # Materialize the old version outside the lake first: once the table is
  # dropped inside the transaction, time travel can no longer resolve it,
  # and CREATE OR REPLACE followed by INSERT in one transaction loses the
  # inserted rows in DuckLake 1.0
  temp_name <- paste0("__ducklake_restore_", gsub("[^a-zA-Z0-9]", "_", table_name))
  temp_table <- quote_ident(paste("temp", "main", temp_name, sep = "."), conn)
  db_execute(sprintf("DROP TABLE IF EXISTS %s;", temp_table), conn = conn)
  on.exit(
    try(
      db_execute(sprintf("DROP TABLE IF EXISTS %s;", temp_table), conn = conn),
      silent = TRUE
    ),
    add = TRUE
  )

  tryCatch({
    db_execute(
      sprintf(
        "CREATE TEMP TABLE %s AS SELECT * FROM %s AT (%s);",
        quote_ident(temp_name, conn), quoted_table, at_clause
      ),
      conn = conn
    )

    with_transaction(
      rebuild_table_from(table_name, temp_table, meta, character(), conn),
      author = author,
      commit_message = commit_message,
      conn = conn
    )
    reapply_table_options(meta, conn)
    invisible(TRUE)
  }, error = function(e) {
    cli::cli_abort(c(
      "Failed to restore table {.val {table_name}}.",
      "x" = e$message,
      "i" = "Check {.code list_table_snapshots(\"{table_name}\")} for available snapshots."
    ))
  })
}
