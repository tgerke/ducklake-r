#' Begin a transaction
#'
#' Starts a new transaction in the DuckDB connection. All subsequent operations
#' will be part of this transaction until it is committed or rolled back.
#'
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @returns Invisibly returns TRUE on success
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
  cli::cli_inform("Transaction started.")
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
#' @returns Invisibly returns TRUE on success
#' @export
#'
#' @details
#' This function commits all changes made since \code{begin_transaction()} was called,
#' making them permanent in the database.
#'
#' If \code{author}, \code{commit_message}, or \code{commit_extra_info} are provided,
#' they will be set using \code{CALL ducklake.set_commit_message()} within the
#' transaction before the \code{COMMIT} statement, as required by the DuckLake
#' v1.0 specification.
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
commit_transaction <- function(
  conn = NULL,
  author = NULL,
  commit_message = NULL,
  commit_extra_info = NULL
) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }

  # In DuckLake v1.0, commit metadata must be set within the transaction
  # using CALL set_commit_message() before COMMIT
  if (
    !is.null(author) || !is.null(commit_message) || !is.null(commit_extra_info)
  ) {
    current_db <- tryCatch(
      DBI::dbGetQuery(conn, "SELECT current_database() as db")$db,
      error = function(e) NULL
    )

    if (!is.null(current_db) && current_db != "") {
      # Build the CALL set_commit_message() statement
      # Signature: CALL ducklake.set_commit_message(author, message, extra_info => '...')
      author_sql <- if (!is.null(author)) {
        sprintf("'%s'", gsub("'", "''", author))
      } else {
        "NULL"
      }
      message_sql <- if (!is.null(commit_message)) {
        sprintf("'%s'", gsub("'", "''", commit_message))
      } else {
        "NULL"
      }

      if (!is.null(commit_extra_info)) {
        extra_sql <- sprintf("'%s'", gsub("'", "''", commit_extra_info))
        call_sql <- sprintf(
          "CALL %s.set_commit_message(%s, %s, extra_info => %s)",
          current_db,
          author_sql,
          message_sql,
          extra_sql
        )
      } else {
        call_sql <- sprintf(
          "CALL %s.set_commit_message(%s, %s)",
          current_db,
          author_sql,
          message_sql
        )
      }

      tryCatch(
        DBI::dbExecute(conn, call_sql),
        error = function(e) {
          cli::cli_warn("Could not set commit metadata: {e$message}")
        }
      )
    } else {
      cli::cli_warn("Could not determine ducklake name; metadata not set.")
    }
  }

  DBI::dbExecute(conn, "COMMIT;")
  cli::cli_inform("Transaction committed.")

  invisible(TRUE)
}

#' Set metadata for the most recent snapshot
#'
#' Sets the author, commit message, and/or extra info for the most recent
#' snapshot in a DuckLake catalog by updating the `ducklake_snapshot_changes`
#' metadata table directly.
#'
#' @param ducklake_name The name of the DuckLake catalog
#' @param author Optional author name to associate with the snapshot
#' @param commit_message Optional commit message describing the changes
#' @param commit_extra_info Optional extra information about the commit
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @returns Invisibly returns TRUE on success
#' @export
#'
#' @details
#' This function retroactively updates metadata on the most recent snapshot.
#' To set metadata at commit time, use the \code{author}, \code{commit_message},
#' and \code{commit_extra_info} arguments in \code{commit_transaction()} or
#' \code{with_transaction()} instead.
#'
#' @examples
#' \dontrun{
#' begin_transaction()
#' # ... make changes ...
#' commit_transaction()
#'
#' # Add metadata to the snapshot after the fact
#' set_snapshot_metadata(
#'   ducklake_name = "my_ducklake",
#'   author = "Data Team",
#'   commit_message = "Updated station names for clarity"
#' )
#' }
set_snapshot_metadata <- function(
  ducklake_name,
  author = NULL,
  commit_message = NULL,
  commit_extra_info = NULL,
  conn = NULL
) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }

  if (
    is.null(author) && is.null(commit_message) && is.null(commit_extra_info)
  ) {
    cli::cli_warn("No metadata provided to set.")
    return(invisible(FALSE))
  }

  # Build parameterized SET clause to prevent SQL injection
  set_parts <- character()
  params <- list()
  if (!is.null(author)) {
    set_parts <- c(set_parts, "author = ?")
    params <- c(params, list(author))
  }
  if (!is.null(commit_message)) {
    set_parts <- c(set_parts, "commit_message = ?")
    params <- c(params, list(commit_message))
  }
  if (!is.null(commit_extra_info)) {
    set_parts <- c(set_parts, "commit_extra_info = ?")
    params <- c(params, list(commit_extra_info))
  }

  # Qualify metadata table names based on backend
  backend <- get_ducklake_backend()
  meta_db <- paste0("__ducklake_metadata_", ducklake_name)
  if (backend %in% c("postgres", "mysql")) {
    changes_ref <- sprintf("\"%s\".ducklake_snapshot_changes", meta_db)
    snapshot_ref <- sprintf("\"%s\".ducklake_snapshot", meta_db)
  } else {
    changes_ref <- sprintf("\"%s\".main.ducklake_snapshot_changes", meta_db)
    snapshot_ref <- sprintf("\"%s\".main.ducklake_snapshot", meta_db)
  }

  update_sql <- sprintf(
    "UPDATE %s SET %s WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM %s)",
    changes_ref,
    paste(set_parts, collapse = ", "),
    snapshot_ref
  )

  tryCatch(
    {
      DBI::dbExecute(conn, update_sql, params = params)
      cli::cli_inform("Snapshot metadata updated.")
      invisible(TRUE)
    },
    error = function(e) {
      cli::cli_warn("Could not update snapshot metadata: {e$message}")
      invisible(FALSE)
    }
  )
}

#' Execute code within a transaction
#'
#' Wraps code execution in a transaction, automatically committing on success
#' or rolling back on error. This provides a more R-idiomatic and safer way to
#' handle transactions compared to manually calling \code{begin_transaction()}
#' and \code{commit_transaction()}.
#'
#' @param expr An R expression or code block to execute within the transaction.
#'   Can be a single statement or a \code{\{...\}} block containing multiple statements.
#' @param author Optional author name to associate with the snapshot
#' @param commit_message Optional commit message describing the changes
#' @param commit_extra_info Optional extra information about the commit
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @returns Invisibly returns the result of the expression
#' @export
#'
#' @details
#' This function provides automatic error handling and cleanup for transactions:
#' \itemize{
#'   \item Begins a transaction before executing the code
#'   \item Executes the provided expression
#'   \item On success: commits the transaction and adds metadata (if provided)
#'   \item On error: automatically rolls back the transaction and re-throws the error
#' }
#'
#' This pattern is similar to \code{withr::with_*()} functions and provides
#' better safety guarantees than manually managing transactions.
#'
#' @examples
#' \dontrun{
#' # Single operation
#' with_transaction(
#'   create_table(mtcars, "cars"),
#'   author = "Data Team",
#'   commit_message = "Add cars dataset"
#' )
#'
#' # Multiple operations in a block
#' with_transaction({
#'   create_table(mtcars, "cars")
#'   create_table(iris, "flowers")
#' }, author = "Data Team", commit_message = "Add datasets")
#'
#' # With dplyr pipeline
#' with_transaction(
#'   get_ducklake_table("cars") |>
#'     mutate(kpl = mpg * 0.425144) |>
#'     replace_table("cars"),
#'   author = "Data Team",
#'   commit_message = "Add km/L column"
#' )
#'
#' # Automatic rollback on error
#' tryCatch(
#'   with_transaction({
#'     create_table(mtcars, "cars")
#'     stop("Simulated error")  # Transaction will be rolled back
#'   }),
#'   error = function(e) message("Transaction was rolled back: ", e$message)
#' )
#' }
with_transaction <- function(
  expr,
  author = NULL,
  commit_message = NULL,
  commit_extra_info = NULL,
  conn = NULL
) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }

  begin_transaction(conn = conn)

  tryCatch(
    {
      result <- force(expr)
      commit_transaction(
        conn = conn,
        author = author,
        commit_message = commit_message,
        commit_extra_info = commit_extra_info
      )
      invisible(result)
    },
    error = function(e) {
      rollback_transaction(conn = conn)
      cli::cli_abort(
        "Transaction rolled back due to error: {e$message}",
        call = NULL
      )
    }
  )
}

#' Rollback a transaction
#'
#' Rolls back the current transaction, discarding all changes made since the
#' transaction began.
#'
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @returns Invisibly returns TRUE on success
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
  cli::cli_inform("Transaction rolled back.")
  invisible(TRUE)
}
