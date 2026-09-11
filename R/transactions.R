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

  # Remember what this connection had committed so far, so the commit can
  # tell a transaction that created a snapshot from one that changed
  # nothing. Read before BEGIN: DuckDB starts the transaction on the lake's
  # catalog at the first statement that touches it, and on a SQLite
  # catalog that holds the file's shared lock until the transaction ends,
  # so the transaction should not touch the lake before the caller does.
  before <- committed_snapshot_id(conn)
  DBI::dbExecute(conn, "BEGIN TRANSACTION;")
  .ducklake_env$txn_committed_before <- before
  invisible(TRUE)
}

#' Commit a transaction
#'
#' Commits the current transaction, making all changes permanent. Optionally adds
#' metadata (author, commit message, and extra info) to the snapshot.
#'
#' @param conn Optional DuckDB connection object. If not provided, uses the default ducklake connection.
#' @param author Author to record on the snapshot. Defaults to the
#'   `ducklake.author` option when it is set (see `?ducklake`), otherwise
#'   none.
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
#' v1.0 specification. An author set once for the session with
#' `options(ducklake.author = "...")` is recorded on every commit that does
#' not name one; the `author` argument wins when both are given.
#'
#' The commit is confirmed with one message naming the snapshot it created,
#' with the author and commit message when they were given. The id comes
#' from DuckLake's `last_committed_snapshot()`, which tracks this
#' connection's own commits, so it is right even when other sessions
#' commit to the same lake at the same time. A transaction that changed
#' nothing creates no snapshot, and the message says so, naming the
#' snapshot the lake stands at. `options(ducklake.verbose = FALSE)`
#' silences these confirmations.
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
  author <- resolve_author(author)

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

  # Report the snapshot this commit made. The template names locals of
  # this frame, so user text is substituted as a value, never parsed as
  # cli markup.
  before <- .ducklake_env$txn_committed_before
  .ducklake_env$txn_committed_before <- NULL
  snapshot <- committed_snapshot_id(conn)
  if (is.na(snapshot) || isTRUE(snapshot == before)) {
    # This transaction committed nothing: say where the lake stands, which
    # can be a neighbor's newer snapshot
    snapshot <- NA_integer_
    current <- current_snapshot_id(conn)
  } else {
    current <- snapshot
  }
  dl_inform(commit_confirmation(snapshot, current, author, commit_message))

  invisible(TRUE)
}

#' Set metadata for a snapshot
#'
#' Fills in the author, commit message, and/or extra info of a snapshot in
#' a DuckLake catalog after it was committed, by updating the
#' `ducklake_snapshot_changes` metadata table directly. The most recent
#' snapshot by default; `snapshot_id` names another.
#'
#' @param ducklake_name The name of the DuckLake catalog
#' @param author Optional author name to associate with the snapshot. The
#'   `ducklake.author` option is not read here: labeling a snapshot after
#'   the fact is a deliberate edit, so the author is always spelled out.
#' @param commit_message Optional commit message describing the changes
#' @param commit_extra_info Optional extra information about the commit
#' @param snapshot_id Optional snapshot id (see [list_table_snapshots()]).
#'   `NULL`, the default, means the most recent snapshot. An id the lake
#'   does not have is an error.
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
#' Snapshot 0 is the lake's creation, which DuckLake writes without an
#' author or a message. [attach_ducklake()] labels it for a lake it
#' creates; for a lake created before that, or attached read-only, pinned
#' to a snapshot, or inside a transaction at the time, pass
#' `snapshot_id = 0` here.
#'
#' It writes to the catalog's metadata table outside DuckLake's transaction
#' and conflict model, and an overwrite leaves no trace of the previous
#' value. That is why it fills blanks only unless `overwrite = TRUE`. Where
#' the snapshot history is the audit trail (GxP, 21 CFR Part 11), set
#' metadata at commit time and leave `overwrite` alone. Call it outside a
#' transaction: a DuckDB transaction can write to one attached database,
#' and this one writes to the metadata catalog, so a lake write after it
#' in the same transaction would fail.
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
#' # The creation snapshot has the message attach_ducklake() gave it and no
#' # author yet: name one
#' set_snapshot_metadata("meta_lake", author = "Data Team", snapshot_id = 0)
#'
#' detach_ducklake("meta_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
set_snapshot_metadata <- function(
  ducklake_name,
  author = NULL,
  commit_message = NULL,
  commit_extra_info = NULL,
  snapshot_id = NULL,
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

  if (is.null(snapshot_id)) {
    snapshot_id <- tryCatch(
      DBI::dbGetQuery(
        conn,
        sprintf(
          "SELECT MAX(snapshot_id) AS id FROM %s.ducklake_snapshot",
          metadata_prefix(ducklake_name, conn)
        )
      )$id,
      error = function(e) {
        cli::cli_warn("Could not read the lake's snapshots: {e$message}")
        NULL
      }
    )
    if (is.null(snapshot_id)) {
      return(invisible(FALSE))
    }
  } else if (
    !is.numeric(snapshot_id) || length(snapshot_id) != 1 ||
      is.na(snapshot_id) || snapshot_id < 0 || snapshot_id != trunc(snapshot_id)
  ) {
    cli::cli_abort(
      "{.arg snapshot_id} must be a single non-negative whole number."
    )
  }
  snapshot_id <- as.integer(snapshot_id)

  # One read serves two checks: the snapshot must exist (a bound UPDATE on
  # a missing id affects no rows and raises nothing), and unless
  # overwriting, the supplied fields must still be empty
  current <- tryCatch(
    read_snapshot_metadata(ducklake_name, snapshot_id, conn),
    error = function(e) NULL
  )
  if (!is.null(current) && nrow(current) == 0) {
    cli::cli_abort(c(
      "Snapshot {snapshot_id} does not exist in {.val {ducklake_name}}.",
      "i" = "{.fn list_table_snapshots} lists the snapshots the lake has."
    ))
  }
  if (!isTRUE(overwrite) && !is.null(current)) {
    taken <- names(provided)[!is.na(unlist(current[1, names(provided)]))]
    if (length(taken) > 0) {
      cli::cli_abort(c(
        "Snapshot {snapshot_id} already has {.field {taken}} set.",
        "i" = "Metadata belongs on the commit: record it with {.fn with_transaction} or {.fn commit_transaction}.",
        "i" = "Pass {.code overwrite = TRUE} to replace {cli::qty(length(taken))}{?it/them}; the previous value{?s} {?is/are} not kept."
      ))
    }
  }

  tryCatch(
    {
      update_snapshot_metadata(ducklake_name, snapshot_id, provided, conn)
      dl_inform("Updated the metadata of snapshot {snapshot_id}.")
      invisible(TRUE)
    },
    error = function(e) {
      cli::cli_warn("Could not update snapshot metadata: {e$message}")
      invisible(FALSE)
    }
  )
}

#' One snapshot's metadata row: author, commit_message, commit_extra_info
#'
#' Zero rows when the snapshot does not exist. Catalog errors propagate, so
#' the caller decides whether they are a warning or an error.
#' @noRd
read_snapshot_metadata <- function(ducklake_name, snapshot_id, conn) {
  DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT author, commit_message, commit_extra_info FROM %s.ducklake_snapshot_changes WHERE snapshot_id = ?",
      metadata_prefix(ducklake_name, conn)
    ),
    params = list(as.integer(snapshot_id))
  )
}

#' Parameterized UPDATE of one snapshot's metadata fields
#'
#' `values` is a named list of any of author, commit_message, and
#' commit_extra_info; only its names reach the SQL text, the values and the
#' id are bound parameters. `condition` is extra SQL ANDed into the WHERE
#' clause.
#'
#' @returns The number of rows updated: 1, or 0 when no row matched.
#' @noRd
update_snapshot_metadata <- function(ducklake_name, snapshot_id, values, conn,
                                     condition = NULL) {
  where <- "snapshot_id = ?"
  if (!is.null(condition)) {
    where <- paste(where, "AND", condition)
  }
  DBI::dbExecute(
    conn,
    sprintf(
      "UPDATE %s.ducklake_snapshot_changes SET %s WHERE %s",
      metadata_prefix(ducklake_name, conn),
      paste0(names(values), " = ?", collapse = ", "),
      where
    ),
    params = c(unname(values), list(as.integer(snapshot_id)))
  )
}

#' Label the creation snapshot of a lake this call just created
#'
#' DuckLake writes snapshot 0 during ATTACH and ignores set_commit_message()
#' around it, so the label is one conditional UPDATE: snapshot 0 takes
#' `author` and `commit_message` only while it is the lake's sole snapshot
#' and carries no metadata, which is how DuckLake leaves a lake it has just
#' created. The rows-affected count says whether that was the case, so
#' there is no probe and no window between probe and write. Silent; a
#' failure warns.
#'
#' @returns Invisibly, `TRUE` when snapshot 0 was labeled.
#' @noRd
label_creation_snapshot <- function(ducklake_name, author, commit_message, conn) {
  values <- list(author = author, commit_message = commit_message)
  values <- values[!vapply(values, is.null, logical(1))]
  if (length(values) == 0) {
    return(invisible(FALSE))
  }
  sole_and_blank <- sprintf(
    "author IS NULL AND commit_message IS NULL AND commit_extra_info IS NULL AND (SELECT COUNT(*) FROM %s.ducklake_snapshot) = 1",
    metadata_prefix(ducklake_name, conn)
  )
  n <- tryCatch(
    update_snapshot_metadata(
      ducklake_name, 0L, values, conn, condition = sole_and_blank
    ),
    error = function(e) {
      cli::cli_warn(c(
        "Could not label the lake's creation snapshot: {e$message}",
        "i" = "Label it later with {.code set_snapshot_metadata('{ducklake_name}', ..., snapshot_id = 0)}."
      ))
      0L
    }
  )
  invisible(n == 1)
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
#' @param author Author to record on the snapshot. Defaults to the
#'   `ducklake.author` option when it is set (see `?ducklake`), otherwise
#'   none.
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
  .ducklake_env$txn_committed_before <- NULL
  dl_inform("Transaction rolled back.")
  invisible(TRUE)
}

#' The lake's current snapshot id, or NA when it cannot be read
#'
#' Inside an open transaction this is the snapshot the transaction started
#' from: DuckLake assigns the next id only at commit.
#' @noRd
current_snapshot_id <- function(conn) {
  tryCatch(
    {
      name <- infer_ducklake_name(NULL, conn)
      id <- DBI::dbGetQuery(
        conn,
        sprintf("FROM %s.current_snapshot()", quote_ident(name, conn))
      )[[1]]
      as.integer(id)
    },
    error = function(e) NA_integer_
  )
}

#' The snapshot this connection last committed, or NA
#'
#' DuckLake's `last_committed_snapshot()` is connection state: NA until the
#' connection commits a change to the lake, then the id of the snapshot its
#' latest writing commit created, unmoved by empty commits, rollbacks, and
#' other connections' commits. That makes it the id a commit confirmation
#' should name. An extension without the function falls back to the lake's
#' current snapshot, the newest commit from any connection.
#' @noRd
committed_snapshot_id <- function(conn) {
  name <- tryCatch(infer_ducklake_name(NULL, conn), error = function(e) NULL)
  if (is.null(name)) {
    return(NA_integer_)
  }
  id <- tryCatch(
    DBI::dbGetQuery(
      conn,
      sprintf("FROM %s.last_committed_snapshot()", quote_ident(name, conn))
    )[[1]],
    error = function(e) {
      missing_function <- grepl(
        "last_committed_snapshot does not exist", conditionMessage(e), fixed = TRUE
      )
      if (missing_function) current_snapshot_id(conn) else NA_integer_
    }
  )
  if (length(id) != 1) NA_integer_ else as.integer(id)
}

#' The cli template for a commit confirmation
#'
#' Returns a template that refers to `snapshot`, `current`, `author`, and
#' `commit_message` in the caller's frame, where `dl_inform()` interpolates
#' them. `snapshot` is the id the transaction committed, or NA when it
#' committed nothing; `current` is then where the lake stands.
#' @noRd
commit_confirmation <- function(snapshot, current, author, commit_message) {
  if (is.na(snapshot)) {
    if (is.na(current)) {
      return("Transaction committed.")
    }
    return("Committed with no changes: the lake stays at snapshot {current}.")
  }
  paste0(
    "Committed snapshot {snapshot}",
    if (!is.null(author)) " ({author})" else "",
    if (!is.null(commit_message)) ": {commit_message}" else "."
  )
}
