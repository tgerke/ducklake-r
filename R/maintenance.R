#' Expire old snapshots
#'
#' Removes old snapshots from the DuckLake catalog. Expiring snapshots gives
#' up the ability to time-travel to them, and schedules the data files that
#' only they referenced for deletion.
#'
#' @param ducklake_name Name of the attached DuckLake catalog. If `NULL`, the
#'   current database is used.
#' @param older_than Expire all snapshots older than this timestamp (POSIXct
#'   or character in ISO 8601 format). POSIXct values are converted to UTC,
#'   which is how DuckLake records snapshot times; character values must
#'   already be UTC. At least one of `older_than` or `versions` must be
#'   provided.
#' @param versions Integer vector of specific snapshot ids to expire (see
#'   [list_table_snapshots()]).
#' @param dry_run If `TRUE`, only lists the snapshots that would be expired
#'   without expiring them.
#'
#' @details
#' Expiring snapshots does not delete any files by itself: files that are no
#' longer referenced are merely scheduled for deletion. Run
#' [cleanup_old_files()] afterwards to reclaim the storage, or let
#' [checkpoint_ducklake()] handle both steps.
#'
#' The most recent snapshot can never be expired.
#'
#' @returns A data frame listing the expired (or, with `dry_run = TRUE`,
#'   expirable) snapshots.
#' @family maintenance
#' @export
#'
#' @seealso [cleanup_old_files()], [checkpoint_ducklake()],
#'   [list_table_snapshots()]
#'
#' @examples
#' \dontrun{
#' # Preview what a one-week retention policy would remove
#' expire_snapshots(older_than = Sys.time() - 7 * 24 * 60 * 60, dry_run = TRUE)
#'
#' # Expire it for real, then reclaim the storage
#' expire_snapshots(older_than = Sys.time() - 7 * 24 * 60 * 60)
#' cleanup_old_files(cleanup_all = TRUE)
#'
#' # Expire two specific snapshots
#' expire_snapshots(versions = c(2, 3))
#' }
expire_snapshots <- function(ducklake_name = NULL,
                             older_than = NULL,
                             versions = NULL,
                             dry_run = FALSE) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  if (is.null(older_than) && is.null(versions)) {
    cli::cli_abort(
      "Must provide either {.arg older_than} or {.arg versions}."
    )
  }

  args <- quote_sql(ducklake_name)
  if (!is.null(versions)) {
    versions <- as.integer(versions)
    if (length(versions) == 0 || anyNA(versions)) {
      cli::cli_abort("{.arg versions} must be one or more snapshot ids.")
    }
    args <- c(args, sprintf(
      "versions => [%s]", paste(versions, collapse = ", ")
    ))
  }
  if (!is.null(older_than)) {
    args <- c(args, sprintf(
      "older_than => TIMESTAMP %s", quote_sql(format_timestamp(older_than))
    ))
  }
  if (isTRUE(dry_run)) {
    args <- c(args, "dry_run => true")
  }

  result <- DBI::dbGetQuery(
    conn,
    sprintf("CALL ducklake_expire_snapshots(%s);", paste(args, collapse = ", "))
  )

  n <- nrow(result)
  if (isTRUE(dry_run)) {
    cli::cli_inform("Dry run: {.val {n}} snapshot{?s} would be expired.")
  } else if (n > 0) {
    cli::cli_inform(c(
      "Expired {.val {n}} snapshot{?s}.",
      "i" = "Unreferenced files are scheduled for deletion; run {.fun cleanup_old_files} to reclaim storage."
    ))
  } else {
    cli::cli_inform("No snapshots to expire.")
  }

  result
}

#' Merge adjacent Parquet files
#'
#' Compacts small adjacent Parquet files into larger ones. Frequent small
#' inserts each write their own file; merging keeps file counts down and
#' scans fast.
#'
#' @param ducklake_name Name of the attached DuckLake catalog. If `NULL`, the
#'   current database is used.
#' @param table_name Optional table name. When provided, only that table is
#'   compacted.
#' @param schema_name Optional schema name. When provided, only tables in
#'   that schema are compacted.
#' @param max_compacted_files Optional cap on the number of compaction
#'   operations per table in a single call.
#' @param min_file_size Optional minimum file size in bytes; smaller files
#'   are excluded from merging.
#' @param max_file_size Optional maximum file size in bytes; files at or
#'   above this size are excluded. Defaults to the lake's target file size.
#'
#' @details
#' Merging does not delete the original small files -- they may still be
#' referenced by older snapshots. They are scheduled for deletion once no
#' snapshot references them; run [cleanup_old_files()] to remove them.
#'
#' @returns A data frame with one row per output file (columns
#'   `schema_name`, `table_name`, `files_processed`, `files_created`).
#' @family maintenance
#' @export
#'
#' @seealso [cleanup_old_files()], [checkpoint_ducklake()],
#'   [flush_inlined_data()]
#'
#' @examples
#' \dontrun{
#' # Compact the whole lake
#' merge_adjacent_files()
#'
#' # Compact one table, only touching files under 10 MB
#' merge_adjacent_files(table_name = "readings", max_file_size = 10e6)
#' }
merge_adjacent_files <- function(ducklake_name = NULL,
                                 table_name = NULL,
                                 schema_name = NULL,
                                 max_compacted_files = NULL,
                                 min_file_size = NULL,
                                 max_file_size = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  args <- quote_sql(ducklake_name)
  if (!is.null(table_name)) {
    args <- c(args, quote_sql(table_name))
  }
  if (!is.null(schema_name)) {
    args <- c(args, sprintf("schema => %s", quote_sql(schema_name)))
  }
  if (!is.null(max_compacted_files)) {
    args <- c(args, sprintf(
      "max_compacted_files => %d", as.integer(max_compacted_files)
    ))
  }
  if (!is.null(min_file_size)) {
    args <- c(args, sprintf("min_file_size => %.0f", as.numeric(min_file_size)))
  }
  if (!is.null(max_file_size)) {
    args <- c(args, sprintf("max_file_size => %.0f", as.numeric(max_file_size)))
  }

  result <- DBI::dbGetQuery(
    conn,
    sprintf(
      "CALL ducklake_merge_adjacent_files(%s);", paste(args, collapse = ", ")
    )
  )

  if (nrow(result) > 0) {
    merged <- sum(result$files_processed)
    cli::cli_inform(
      "Merged {.val {merged}} file{?s} into {.val {nrow(result)}} file{?s}."
    )
  } else {
    cli::cli_inform("No adjacent files to merge.")
  }

  result
}

#' Build and run a ducklake file-cleanup CALL
#'
#' Shared implementation for [cleanup_old_files()] and
#' [delete_orphaned_files()], which take identical arguments.
#'
#' @param sql_function The DuckLake function to call.
#' @param what Noun for the message, e.g. "old file".
#' @noRd
run_file_cleanup <- function(sql_function, what,
                             ducklake_name, older_than, cleanup_all, dry_run) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  if (is.null(older_than) && !isTRUE(cleanup_all)) {
    cli::cli_abort(
      "Must provide {.arg older_than} or set {.arg cleanup_all} to {.val TRUE}."
    )
  }
  if (!is.null(older_than) && isTRUE(cleanup_all)) {
    cli::cli_abort(
      "Cannot provide both {.arg older_than} and {.arg cleanup_all}."
    )
  }

  args <- quote_sql(ducklake_name)
  if (isTRUE(cleanup_all)) {
    args <- c(args, "cleanup_all => true")
  } else {
    args <- c(args, sprintf(
      "older_than => TIMESTAMP %s", quote_sql(format_timestamp(older_than))
    ))
  }
  if (isTRUE(dry_run)) {
    args <- c(args, "dry_run => true")
  }

  result <- DBI::dbGetQuery(
    conn,
    sprintf("CALL %s(%s);", sql_function, paste(args, collapse = ", "))
  )

  n <- nrow(result)
  if (isTRUE(dry_run)) {
    cli::cli_inform("Dry run: {.val {n}} {cli::qty(n)}{what}{?s} would be deleted.")
  } else {
    cli::cli_inform("Deleted {.val {n}} {cli::qty(n)}{what}{?s}.")
  }

  result
}

#' Delete files scheduled for removal
#'
#' Physically deletes data files that are no longer referenced by any
#' snapshot -- typically files orphaned by [expire_snapshots()] or replaced
#' by [merge_adjacent_files()].
#'
#' @param ducklake_name Name of the attached DuckLake catalog. If `NULL`, the
#'   current database is used.
#' @param older_than Only delete files scheduled for deletion before this
#'   timestamp (POSIXct, converted to UTC, or character already in UTC). One
#'   of `older_than` or `cleanup_all` is required.
#' @param cleanup_all If `TRUE`, delete all scheduled files regardless of
#'   when they were scheduled.
#' @param dry_run If `TRUE`, only lists the files that would be deleted.
#'
#' @details
#' As an alternative to calling this manually, a retention policy can be set
#' once on the catalog with
#' `DBI::dbExecute(get_ducklake_connection(), "CALL my_lake.set_option('delete_older_than', '1 week')")`,
#' after which DuckLake cleans up eligible files automatically.
#'
#' @returns A data frame listing the deleted (or deletable) files.
#' @family maintenance
#' @export
#'
#' @seealso [expire_snapshots()], [delete_orphaned_files()],
#'   [checkpoint_ducklake()]
#'
#' @examples
#' \dontrun{
#' # Preview, then delete everything that is scheduled
#' cleanup_old_files(dry_run = TRUE, cleanup_all = TRUE)
#' cleanup_old_files(cleanup_all = TRUE)
#'
#' # Only delete files scheduled more than a week ago
#' cleanup_old_files(older_than = Sys.time() - 7 * 24 * 60 * 60)
#' }
cleanup_old_files <- function(ducklake_name = NULL,
                              older_than = NULL,
                              cleanup_all = FALSE,
                              dry_run = FALSE) {
  run_file_cleanup(
    "ducklake_cleanup_old_files", "old file",
    ducklake_name, older_than, cleanup_all, dry_run
  )
}

#' Delete orphaned files
#'
#' Deletes files sitting in the lake's data path that are not tracked in the
#' DuckLake metadata at all -- for example, leftovers from a crashed write.
#'
#' @inheritParams cleanup_old_files
#'
#' @details
#' This differs from [cleanup_old_files()], which removes files that *were*
#' tracked but are scheduled for deletion.
#'
#' Always run with `dry_run = TRUE` first and check the file list. Anything
#' in the data path that DuckLake does not recognise is fair game, and the
#' comparison is by exact path string: a data path registered with an
#' irregularity such as a doubled slash (as R's [tempdir()] produces on
#' macOS) makes *live* files look orphaned, and deleting them breaks the
#' lake.
#'
#' @returns A data frame listing the deleted (or deletable) files.
#' @family maintenance
#' @export
#'
#' @seealso [cleanup_old_files()], [checkpoint_ducklake()]
#'
#' @examples
#' \dontrun{
#' # Always preview orphan deletion first
#' delete_orphaned_files(dry_run = TRUE, cleanup_all = TRUE)
#' delete_orphaned_files(cleanup_all = TRUE)
#' }
delete_orphaned_files <- function(ducklake_name = NULL,
                                  older_than = NULL,
                                  cleanup_all = FALSE,
                                  dry_run = FALSE) {
  run_file_cleanup(
    "ducklake_delete_orphaned_files", "orphaned file",
    ducklake_name, older_than, cleanup_all, dry_run
  )
}

#' Rewrite data files with many deletes
#'
#' Rewrites Parquet files whose rows have mostly been deleted. Deletes in
#' DuckLake are recorded in separate delete files; heavily-deleted data files
#' slow reads down until they are rewritten without the dead rows.
#'
#' @param ducklake_name Name of the attached DuckLake catalog. If `NULL`, the
#'   current database is used.
#' @param table_name Optional table name. When provided, only that table's
#'   files are rewritten.
#' @param delete_threshold Optional fraction of deleted rows (between 0 and
#'   1) above which a file is rewritten. DuckLake's default is 0.95.
#'
#' @details
#' The rewritten originals are scheduled for deletion once no snapshot
#' references them; run [cleanup_old_files()] to remove them.
#'
#' @returns A data frame with one row per output file (columns
#'   `schema_name`, `table_name`, `files_processed`, `files_created`).
#' @family maintenance
#' @export
#'
#' @seealso [cleanup_old_files()], [checkpoint_ducklake()]
#'
#' @examples
#' \dontrun{
#' # Rewrite any file that is at least half deleted
#' rewrite_data_files("my_lake", delete_threshold = 0.5)
#'
#' # Just one table, with DuckLake's default threshold
#' rewrite_data_files(table_name = "events")
#' }
rewrite_data_files <- function(ducklake_name = NULL,
                               table_name = NULL,
                               delete_threshold = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  args <- quote_sql(ducklake_name)
  if (!is.null(table_name)) {
    args <- c(args, quote_sql(table_name))
  }
  if (!is.null(delete_threshold)) {
    delete_threshold <- as.numeric(delete_threshold)
    if (is.na(delete_threshold) || delete_threshold < 0 || delete_threshold > 1) {
      cli::cli_abort("{.arg delete_threshold} must be a number between 0 and 1.")
    }
    args <- c(args, sprintf("delete_threshold => %s", delete_threshold))
  }

  # The CALL streams its result by re-scanning the table, which DuckLake
  # rejects once the CALL's implicit transaction has ended ("Scanning a
  # DuckLake table after the transaction has ended"). An explicit
  # transaction keeps the scan valid while the result is fetched.
  db_execute("BEGIN TRANSACTION;", conn = conn)
  result <- tryCatch(
    DBI::dbGetQuery(
      conn,
      sprintf(
        "CALL ducklake_rewrite_data_files(%s);", paste(args, collapse = ", ")
      )
    ),
    error = function(e) {
      try(db_execute("ROLLBACK;", conn = conn), silent = TRUE)
      stop(e)
    }
  )
  db_execute("COMMIT;", conn = conn)

  if (nrow(result) > 0) {
    rewritten <- sum(result$files_processed)
    cli::cli_inform(
      "Rewrote {.val {rewritten}} file{?s} into {.val {nrow(result)}} file{?s}."
    )
  } else {
    cli::cli_inform("No files needed rewriting.")
  }

  result
}
