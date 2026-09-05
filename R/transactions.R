#' Begin a transaction
#'
#' Starts a new transaction in the DuckDB connection. All subsequent operations
#' will be part of this transaction until it is committed or rolled back.
#'
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#'
#' @returns Invisibly returns TRUE on success
#' @family transactions
#' @export
#'
#' @details
#' Transactions allow you to group multiple operations together and ensure they
#' either all succeed or all fail. Use \code{commit_transaction()} to apply the
#' changes or \code{rollback_transaction()} to discard them.
#'
#' DuckDB supports full ACID transactions with multiple isolation levels.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("begin_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("begin_lake", lake_path = lake_dir)
#' create_table(data.frame(id = 1:3, status = "pending"), "jobs")
#'
#' # Start a transaction
#' begin_transaction()
#'
#' # Make some changes
#' get_ducklake_table("jobs") |>
#'   dplyr::filter(status == "pending") |>
#'   dplyr::mutate(status = "processed") |>
#'   ducklake_exec()
#'
#' # Commit if everything looks good
#' commit_transaction()
#'
#' # Or rollback if something went wrong
#' # rollback_transaction()
#'
#' detach_ducklake("begin_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
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
#' @family transactions
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
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("commit_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("commit_lake", lake_path = lake_dir)
#'
#' # Basic commit
#' begin_transaction()
#' create_table(iris, "flowers")
#' commit_transaction()
#'
#' # Commit with metadata
#' begin_transaction()
#' create_table(mtcars, "cars")
#' commit_transaction(
#'   author = "John Doe",
#'   commit_message = "Add cars dataset"
#' )
#'
#' detach_ducklake("commit_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
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

    db_ok <- !is.null(current_db) && current_db != "" &&
      grepl("^[A-Za-z_][A-Za-z0-9_]*$", current_db)

    if (db_ok) {
      # Build the CALL set_commit_message() statement
      # Signature: CALL ducklake.set_commit_message(author, message, extra_info => '...')
      author_sql <- if (!is.null(author)) quote_sql(author) else "NULL"
      message_sql <- if (!is.null(commit_message)) {
        quote_sql(commit_message)
      } else {
        "NULL"
      }

      if (!is.null(commit_extra_info)) {
        extra_sql <- quote_sql(commit_extra_info)
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
      cli::cli_warn("Could not determine a usable ducklake name; metadata not set.")
    }
  }

  DBI::dbExecute(conn, "COMMIT;")
  cli::cli_inform("Transaction committed.")

  invisible(TRUE)
}

#' Set metadata for the most recent snapshot
#'
#' Fills in the author, commit message, and/or extra info of the most
#' recent snapshot in a DuckLake catalog after it was committed, by
#' updating the `ducklake_snapshot_changes` metadata table directly.
#'
#' @param ducklake_name The name of the DuckLake catalog
#' @param author Optional author name to associate with the snapshot
#' @param commit_message Optional commit message describing the changes
#' @param commit_extra_info Optional extra information about the commit
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#' @param overwrite Replace values the snapshot already carries (default
#'   `FALSE`). By default only empty fields are filled in, and the call
#'   stops when a supplied field already has a value.
#'
#' @returns Invisibly returns TRUE on success
#' @family transactions
#' @export
#'
#' @details
#' Metadata belongs on the commit: pass `author`, `commit_message`, and
#' `commit_extra_info` to [with_transaction()] or [commit_transaction()],
#' which record them through DuckLake's `set_commit_message()` as part of
#' the transaction itself. This function is the escape hatch for a snapshot
#' that was committed without them, such as one made interactively or by a
#' client that could not set them.
#'
#' It writes to the catalog's metadata table outside DuckLake's transaction
#' and conflict model, and an overwrite leaves no trace of the previous
#' value. That is why it fills blanks only unless `overwrite = TRUE`. Where
#' the snapshot history is the audit trail (GxP, 21 CFR Part 11), set
#' metadata at commit time and leave `overwrite` alone.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("meta_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("meta_lake", lake_path = lake_dir)
#'
#' begin_transaction()
#' create_table(mtcars, "cars")
#' commit_transaction()
#'
#' # The snapshot has no author or message yet: fill them in
#' set_snapshot_metadata(
#'   ducklake_name = "meta_lake",
#'   author = "Data Team",
#'   commit_message = "Added the cars dataset"
#' )
#'
#' # A second call refuses to replace them unless told to
#' try(set_snapshot_metadata("meta_lake", commit_message = "Reworded"))
#' set_snapshot_metadata("meta_lake", commit_message = "Reworded", overwrite = TRUE)
#'
#' detach_ducklake("meta_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
set_snapshot_metadata <- function(
  ducklake_name,
  author = NULL,
  commit_message = NULL,
  commit_extra_info = NULL,
  conn = NULL,
  overwrite = FALSE
) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  check_identifier(ducklake_name)

  provided <- list(
    author = author,
    commit_message = commit_message,
    commit_extra_info = commit_extra_info
  )
  provided <- provided[!vapply(provided, is.null, logical(1))]
  if (length(provided) == 0) {
    cli::cli_warn("No metadata provided to set.")
    return(invisible(FALSE))
  }

  prefix <- metadata_prefix(ducklake_name, conn)
  changes_ref <- paste0(prefix, ".ducklake_snapshot_changes")
  latest <- sprintf(
    "(SELECT MAX(snapshot_id) FROM %s.ducklake_snapshot)", prefix
  )

  if (!isTRUE(overwrite)) {
    current <- tryCatch(
      DBI::dbGetQuery(
        conn,
        sprintf(
          "SELECT author, commit_message, commit_extra_info FROM %s WHERE snapshot_id = %s",
          changes_ref, latest
        )
      ),
      error = function(e) NULL
    )
    if (!is.null(current) && nrow(current) == 1) {
      taken <- names(provided)[!is.na(unlist(current[1, names(provided)]))]
      if (length(taken) > 0) {
        cli::cli_abort(c(
          "The latest snapshot already has {.field {taken}} set.",
          "i" = "Metadata belongs on the commit: record it with {.fn with_transaction} or {.fn commit_transaction}.",
          "i" = "Pass {.code overwrite = TRUE} to replace {cli::qty(length(taken))}{?it/them}; the previous value{?s} {?is/are} not kept."
        ))
      }
    }
  }

  # Parameterized SET clause: values never touch the SQL text
  update_sql <- sprintf(
    "UPDATE %s SET %s WHERE snapshot_id = %s",
    changes_ref,
    paste0(names(provided), " = ?", collapse = ", "),
    latest
  )

  tryCatch(
    {
      DBI::dbExecute(conn, update_sql, params = unname(provided))
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
#' @family transactions
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
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("with_tx_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("with_tx_lake", lake_path = lake_dir)
#'
#' # Single operation
#' with_transaction(
#'   create_table(mtcars, "cars"),
#'   author = "Data Team",
#'   commit_message = "Add cars dataset"
#' )
#'
#' # Multiple operations in a block
#' with_transaction({
#'   create_table(iris, "flowers")
#'   create_table(airquality, "air")
#' }, author = "Data Team", commit_message = "Add datasets")
#'
#' # With dplyr pipeline
#' with_transaction(
#'   get_ducklake_table("cars") |>
#'     dplyr::mutate(kpl = mpg * 0.425144) |>
#'     replace_table("cars"),
#'   author = "Data Team",
#'   commit_message = "Add km/L column"
#' )
#'
#' # Automatic rollback on error
#' tryCatch(
#'   with_transaction({
#'     create_table(ChickWeight, "chicks")
#'     stop("Simulated error") # Transaction will be rolled back
#'   }),
#'   error = function(e) message("Transaction was rolled back: ", e$message)
#' )
#'
#' # "chicks" was never committed
#' list_ducklake_tables()
#'
#' detach_ducklake("with_tx_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
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
#' @family transactions
#' @export
#'
#' @details
#' This function discards all changes made since \code{begin_transaction()} was called,
#' reverting the database to its state before the transaction began.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("rollback_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("rollback_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' begin_transaction()
#' rows_delete(
#'   get_ducklake_table("cars"),
#'   data.frame(gear = 3),
#'   by = "gear"
#' )
#'
#' # Something went wrong, rollback
#' rollback_transaction()
#'
#' detach_ducklake("rollback_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
rollback_transaction <- function(conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }

  DBI::dbExecute(conn, "ROLLBACK;")
  cli::cli_inform("Transaction rolled back.")
  invisible(TRUE)
}
