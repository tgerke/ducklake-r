#' Begin a transaction
#'
#' Starts a new transaction in the DuckDB connection. All subsequent operations
#' will be part of this transaction until it is committed or rolled back.
#'
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @return Invisibly returns TRUE on success
#' @export
#'
#' @details
#' Transactions allow you to group multiple operations together and ensure they
#' either all succeed or all fail. Use \code{commit_transaction()} to apply the
#' changes or \code{rollback_transaction()} to discard them.
#'
#' DuckDB supports full ACID transactions with multiple isolation levels.
#'
#' @examples
#' \dontrun{
#' # Start a transaction
#' begin_transaction()
#'
#' # Make some changes
#' get_ducklake_table("my_table") |>
#'   filter(status == "pending") |>
#'   mutate(status = "processed") |>
#'   ducklake_exec()
#'
#' # Commit if everything looks good
#' commit_transaction()
#'
#' # Or rollback if something went wrong
#' # rollback_transaction()
#' }
begin_transaction <- function(conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  DBI::dbExecute(conn, "BEGIN TRANSACTION;")
  message("Transaction started")
  invisible(TRUE)
}

#' Commit a transaction
#'
#' Commits the current transaction, making all changes permanent. Optionally adds
#' metadata (author, commit message, and extra info) to the snapshot.
#'
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#' @param author Optional author name to associate with the snapshot
#' @param commit_message Optional commit message describing the changes
#' @param commit_extra_info Optional extra information about the commit
#'
#' @return Invisibly returns TRUE on success
#' @export
#'
#' @details
#' This function commits all changes made since \code{begin_transaction()} was called,
#' making them permanent in the database. DuckLake automatically tracks changes
#' in the \code{ducklake_snapshot_changes} metadata table.
#'
#' If \code{author}, \code{commit_message}, or \code{commit_extra_info} are provided,
#' they will be automatically added to the snapshot metadata after committing.
#'
#' @examples
#' \dontrun{
#' # Basic commit
#' begin_transaction()
#' # ... make changes ...
#' commit_transaction()
#' 
#' # Commit with metadata
#' begin_transaction()
#' create_table(mtcars, "cars")
#' commit_transaction(
#'   author = "John Doe",
#'   commit_message = "Add cars dataset"
#' )
#' }
commit_transaction <- function(conn = NULL, author = NULL, commit_message = NULL,
                                commit_extra_info = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  DBI::dbExecute(conn, "COMMIT;")
  message("Transaction committed")
  
  # Add metadata if any is provided
  if (!is.null(author) || !is.null(commit_message) || !is.null(commit_extra_info)) {
    # Get the current database name
    current_db <- DBI::dbGetQuery(conn, "SELECT current_database() as db")$db
    
    if (!is.null(current_db) && current_db != "") {
      set_snapshot_metadata(
        ducklake_name = current_db,
        author = author,
        commit_message = commit_message,
        commit_extra_info = commit_extra_info,
        conn = conn
      )
    } else {
      warning("Could not determine ducklake name; metadata not set")
    }
  }
  
  invisible(TRUE)
}

#' Set metadata for the most recent snapshot
#'
#' Updates the author, commit message, and/or extra info for the most recent
#' snapshot in a DuckLake catalog.
#'
#' @param ducklake_name The name of the DuckLake catalog
#' @param author Optional author name to associate with the snapshot
#' @param commit_message Optional commit message describing the changes
#' @param commit_extra_info Optional extra information about the commit
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @return Invisibly returns TRUE on success
#' @export
#'
#' @details
#' This function updates the metadata columns in the \code{ducklake_snapshot_changes}
#' table for the most recent snapshot. Call this after \code{commit_transaction()}
#' to add audit information to your commits.
#'
#' @examples
#' \dontrun{
#' begin_transaction()
#' # ... make changes ...
#' commit_transaction()
#' 
#' # Add metadata to the snapshot
#' set_snapshot_metadata(
#'   ducklake_name = "my_ducklake",
#'   author = "Data Team",
#'   commit_message = "Updated station names for clarity"
#' )
#' }
set_snapshot_metadata <- function(ducklake_name, author = NULL, commit_message = NULL,
                                   commit_extra_info = NULL, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  metadata_db <- sprintf("__ducklake_metadata_%s", ducklake_name)
  
  # Build the SET clause
  set_parts <- character()
  if (!is.null(author)) {
    set_parts <- c(set_parts, sprintf("author = '%s'", author))
  }
  if (!is.null(commit_message)) {
    set_parts <- c(set_parts, sprintf("commit_message = '%s'", commit_message))
  }
  if (!is.null(commit_extra_info)) {
    set_parts <- c(set_parts, sprintf("commit_extra_info = '%s'", commit_extra_info))
  }
  
  if (length(set_parts) == 0) {
    warning("No metadata provided to set")
    return(invisible(FALSE))
  }
  
  set_clause <- paste(set_parts, collapse = ", ")
  
  # Update the most recent snapshot
  query <- sprintf(
    "UPDATE %s.main.ducklake_snapshot_changes SET %s WHERE snapshot_id = (SELECT max(snapshot_id) FROM %s.main.ducklake_snapshot)",
    metadata_db, set_clause, metadata_db
  )
  
  tryCatch({
    DBI::dbExecute(conn, query)
    message("Snapshot metadata updated")
    invisible(TRUE)
  }, error = function(e) {
    warning("Could not update snapshot metadata: ", e$message)
    invisible(FALSE)
  })
}

#' Rollback a transaction
#'
#' Rolls back the current transaction, discarding all changes made since the
#' transaction began.
#'
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @return Invisibly returns TRUE on success
#' @export
#'
#' @details
#' This function discards all changes made since \code{begin_transaction()} was called,
#' reverting the database to its state before the transaction began.
#'
#' @examples
#' \dontrun{
#' begin_transaction()
#' # ... make changes ...
#' # Something went wrong, rollback
#' rollback_transaction()
#' }
rollback_transaction <- function(conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  
  DBI::dbExecute(conn, "ROLLBACK;")
  message("Transaction rolled back")
  invisible(TRUE)
}
