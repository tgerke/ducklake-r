#' Replace a table with modified data and create a new snapshot
#'
#' @param .data The new contents: a dplyr pipeline (a lazy table), a data
#'   frame, or a file path or URL, anything [create_table()] loads.
#' @param table_name The table to replace. It must exist: a name that
#'   matches nothing, or that names a view, is refused. For a call that
#'   creates the table when it is missing and replaces it otherwise, see
#'   [create_table()] with `replace = TRUE`.
#' @param .quiet Logical, whether to suppress messages (default TRUE)
#'
#' @returns Invisibly returns NULL
#' @family table operations
#' @export
#'
#' @details
#' This function is designed for bulk transformations that should create a
#' new versioned snapshot. A dplyr pipeline on the package's connection
#' runs inside DuckDB: its result is materialized in DuckDB's temporary
#' storage (the query may read the table being replaced), the table is
#' dropped and recreated from it, and the metadata DuckLake keeps against
#' the table is put back. No rows pass through R. A data frame, a lazy
#' table on another connection, or a file is loaded the way
#' [create_table()] loads it.
#'
#' The drop and create run atomically: when no transaction is open,
#' `replace_table()` wraps them in one of its own, so a failed create never
#' leaves the table dropped. Wrap the call in [with_transaction()] (or
#' `begin_transaction()`/`commit_transaction()`) when you want to record an
#' author and commit message on the snapshot, or to group the replacement
#' with other changes.
#'
#' DuckLake gives the replacement a new table id, as it does for any
#' `DROP` + `CREATE`, and it stores comments, partition keys, sort order,
#' and table-scoped options against that id. `replace_table()` carries them
#' over inside the same transaction: the table comment, column comments
#' (and so variable labels) for columns that still exist, partition and
#' sort keys whose columns still exist, and options set with
#' [set_ducklake_option()] at table scope, all put on the empty table
#' before the rows are written, so the rewrite itself lands partitioned,
#' sorted, and governed by the options, as one snapshot. Earlier snapshots
#' keep the earlier id and stay reachable by name through time travel.
#'
#' What the open transaction has done to the table counts. Comments set
#' earlier in it are carried over, and so are keys set or reset there with
#' [set_table_partitioning()], [set_table_sorting()], and their `reset_`
#' counterparts. DuckLake shows pending keys nowhere, so ducklake notes
#' them as those functions run: keys changed in the same transaction with a
#' raw `ALTER TABLE` statement are not seen, and the rewrite then carries
#' the committed keys over instead.
#'
#' **When to use replace_table():**
#' - **Bulk transformations** - a dplyr pipeline that recomputes, reshapes,
#'   or filters most of the table
#'
#' **When to reach elsewhere:**
#' - **A script that must also work on its first run** - [create_table()]
#'   with `replace = TRUE` creates the table when it is missing and
#'   rewrites it as this function does otherwise
#' - **Schema-only changes** - [add_table_column()], [drop_table_column()],
#'   [rename_table_column()], and [set_column_type()] alter the table in
#'   place; nothing is collected or rewritten
#' - **Derived columns** - [add_table_column()] followed by a
#'   `mutate()` pipeline through [ducklake_exec()] fills the new column
#'   with an in-database UPDATE
#' - **Targeted row changes** - [rows_update()], [rows_upsert()], or
#'   [ducklake_exec()] modify only the affected rows
#'
#' Both paths create a snapshot: replace_table() via DROP + CREATE, and
#' ducklake_exec() via the in-place UPDATE/DELETE/INSERT it runs, so either
#' way the change is available for time travel.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("replace_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("replace_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' # Add new derived columns (atomic on its own; creates a new snapshot)
#' get_ducklake_table("cars") |>
#'   dplyr::mutate(
#'     thirsty = dplyr::if_else(mpg < 20, "Y", "N"),
#'     mpg_band = dplyr::case_when(
#'       mpg < 15 ~ "<15",
#'       mpg < 25 ~ "15-24",
#'       TRUE ~ ">=25"
#'     )
#'   ) |>
#'   replace_table("cars")
#'
#' # Wrap in with_transaction() to record audit metadata on the snapshot
#' with_transaction(
#'   get_ducklake_table("cars") |>
#'     dplyr::select(-thirsty, -mpg_band) |>
#'     replace_table("cars"),
#'   author = "Data Engineer",
#'   commit_message = "Drop derived columns"
#' )
#'
#' # Partition keys, sort order, comments, and table options survive the rewrite
#' set_table_partitioning("cars", "cyl")
#' get_ducklake_table("cars") |>
#'   dplyr::filter(mpg > 15) |>
#'   replace_table("cars")
#' get_table_partitions("cars")
#'
#' detach_ducklake("replace_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
replace_table <- function(.data, table_name, .quiet = TRUE) {

  if (!.quiet) {
    dl_inform("Replacing table {.val {table_name}}...")
  }

  conn <- get_ducklake_connection()

  # The lookup runs before any statement, so a caller's open transaction
  # is never aborted by a DROP that DuckDB refuses. A missing name used to
  # be created by the DROP IF EXISTS + CREATE sequence, which turned a typo
  # into a new table while the real one went stale.
  existing <- lake_object_type(table_name, conn)
  if (identical(existing, "view")) {
    cli::cli_abort(c(
      "{.val {table_name}} is a view, not a table.",
      "i" = "Store a pipeline as a view with {.fun create_view}, or drop the view with {.fun drop_view} to make room for a table."
    ))
  }
  if (is.na(existing)) {
    cli::cli_abort(c(
      "Table {.val {table_name}} does not exist.",
      "i" = "{.fun replace_table} rewrites an existing table. To create it when it is missing and replace it otherwise, use {.code create_table(replace = TRUE)}."
    ))
  }

  rewrite_table(.data, table_name, quiet = .quiet, conn = conn)

  if (!.quiet) {
    dl_inform("Table {.val {table_name}} successfully replaced.")
  }

  invisible(NULL)
}

#' Rewrite an existing table from new contents, keeping its metadata
#'
#' The sequence behind [replace_table()] and `create_table(replace = TRUE)`:
#' capture what DuckLake keys to the table id, stage the new contents
#' (inside DuckDB when the source is a lazy table on this connection, since
#' it may read the table being rewritten), then drop, recreate, and put the
#' metadata back, in the caller's open transaction or one of this
#' function's own. The source can be anything [create_table()] loads.
#'
#' @param data_source A lazy table, a data frame, or a file path or URL.
#' @param table_name The table to rewrite; the caller has checked it exists.
#' @param labels Whether the rebuilt columns get the source's labels (a data
#'   frame) or inherited comments (a lazy table); the captured comments
#'   fill the remaining columns either way.
#' @param layout The list from `check_table_layout()`; each piece given
#'   replaces the captured one.
#' @param quiet Whether to suppress the row-count message.
#' @param conn The package connection.
#' @returns Invisibly, the names of the columns whose labels were stored.
#' @noRd
rewrite_table <- function(data_source, table_name, labels = TRUE,
                          layout = list(), quiet = TRUE,
                          conn = get_ducklake_connection()) {
  # DROP + CREATE assigns a new table id, and DuckLake keys comments,
  # partition keys, sort order, and table-scoped options to the id: capture
  # them first so they can be put back on the new table
  meta <- capture_table_metadata(table_name, conn = conn)

  if (inherits(data_source, "tbl_lazy") && !same_connection(data_source, conn)) {
    # A lazy table on another connection has to come through R
    data_source <- dplyr::collect(data_source)
  }

  if (inherits(data_source, "tbl_lazy")) {
    # In-database path: the query's result waits in a temporary table
    # while the target is rebuilt, since the query may read the target
    comments <- if (isTRUE(labels)) {
      source_column_comments(data_source, conn)
    } else {
      character()
    }
    source_ref <- materialize_query(data_source, table_name, conn)
    on.exit(
      try(db_execute(sprintf("DROP TABLE IF EXISTS %s;", source_ref), conn = conn), silent = TRUE),
      add = TRUE
    )
  } else if (is.data.frame(data_source)) {
    prepared <- prepare_data_frame(data_source, labels)
    comments <- prepared$labels
    temp_view_name <- register_temp_view(prepared$data, table_name, conn)
    source_ref <- quote_ident(temp_view_name, conn)
    on.exit(
      duckdb::duckdb_unregister(get_ducklake_connection(), temp_view_name),
      add = TRUE
    )
  } else if (is.character(data_source)) {
    # A file or URL is read straight into the rebuilt table, as on a create
    if (grepl("^https?://", data_source)) {
      load_or_install_extension("httpfs")
    }
    comments <- character()
    source_ref <- quote_sql(data_source)
  } else {
    cli::cli_abort(
      "{.arg data_source} must be a lazy table, a data frame, or a character string (file path or URL)."
    )
  }

  if (!quiet) {
    n <- DBI::dbGetQuery(conn, sprintf("SELECT count(*) AS n FROM %s", source_ref))$n
    dl_inform("Prepared {n} row{?s} for {.val {table_name}}.")
  }

  # The drop and create must land together: outside a transaction they
  # autocommit separately, so a failed create would leave the table gone.
  # When the caller already opened a transaction, they own the
  # commit/rollback decision.
  own_txn <- !in_transaction(conn)
  committed <- FALSE
  if (own_txn) {
    DBI::dbExecute(conn, "BEGIN TRANSACTION;")
    # Runs before the cleanup handlers above: DuckDB temp tables are
    # transactional, so a rollback after the temp DROP would bring the
    # temporary copy back
    on.exit(
      if (!committed) {
        tryCatch(DBI::dbExecute(conn, "ROLLBACK;"), error = function(e) NULL)
      },
      add = TRUE, after = FALSE
    )
  }

  rebuild_table_from(table_name, source_ref, meta, comments, conn, layout = layout)

  if (own_txn) {
    DBI::dbExecute(conn, "COMMIT;")
    committed <- TRUE
  }

  invisible(names(comments))
}
