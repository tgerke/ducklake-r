#' Replace a table with modified data and create a new snapshot
#'
#' @param .data A dplyr query object (tbl_lazy) with transformations
#' @param table_name Table name to replace
#' @param .quiet Logical, whether to suppress messages (default TRUE)
#'
#' @returns Invisibly returns NULL
#' @family table operations
#' @export
#'
#' @details
#' This function is designed for schema changes or bulk transformations that should
#' create a new versioned snapshot. It:
#' 1. Collects the transformed data
#' 2. Drops the existing table
#' 3. Creates a new table with the updated schema/data
#' 4. Puts back the metadata DuckLake keeps against the table
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
#' over: the table comment, column comments (and so variable labels) for
#' columns that still exist, partition and sort keys whose columns still
#' exist (set before the rows are written, so the rewrite itself lands
#' partitioned and sorted), and options set with [set_ducklake_option()] at
#' table scope. DuckLake cannot set options on a table created in the open
#' transaction, so options are re-set right after the rewrite commits, as a
#' small follow-up snapshot; inside a transaction you opened yourself they
#' cannot be re-set at all, and a warning lists the calls to make after
#' your commit. Earlier snapshots keep the earlier id and stay reachable by
#' name through time travel.
#'
#' **When to use replace_table():**
#' - **Bulk transformations** - a dplyr pipeline that recomputes, reshapes,
#'   or filters most of the table
#'
#' **When to reach elsewhere:**
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
    cli::cli_inform("Replacing table {.val {table_name}}...")
  }

  # Collect the transformed data
  new_data <- dplyr::collect(.data)

  if (!.quiet) {
    cli::cli_inform(
      "Collected {nrow(new_data)} row{?s} with {ncol(new_data)} column{?s}."
    )
  }

  conn <- get_ducklake_connection()

  # DROP + CREATE assigns a new table id, and DuckLake keys comments,
  # partition keys, sort order, and table-scoped options to the id: capture
  # them first so they can be put back on the new table
  meta <- capture_table_metadata(table_name, conn = conn)

  prepared <- prepare_data_frame(new_data)
  temp_view_name <- register_temp_view(prepared$data, table_name, conn)
  on.exit(
    duckdb::duckdb_unregister(get_ducklake_connection(), temp_view_name),
    add = TRUE
  )

  # The drop and create must land together: outside a transaction they
  # autocommit separately, so a failed create would leave the table gone.
  # When the caller already opened a transaction, they own the
  # commit/rollback decision.
  own_txn <- !in_transaction(conn)
  committed <- FALSE
  if (own_txn) {
    DBI::dbExecute(conn, "BEGIN TRANSACTION;")
    on.exit(
      if (!committed) {
        tryCatch(DBI::dbExecute(conn, "ROLLBACK;"), error = function(e) NULL)
      },
      add = TRUE
    )
  }

  quoted_table <- quote_ident(table_name, conn)
  quoted_view <- quote_ident(temp_view_name, conn)

  # Drop the existing table
  db_execute(sprintf("DROP TABLE IF EXISTS %s", quoted_table), conn = conn)

  # Create the table empty, put the partition and sort keys on it, then
  # insert: DuckLake refuses to alter a table that already holds inlined
  # rows from the open transaction, and keys set first shape the new files
  db_execute(
    sprintf("CREATE TABLE %s AS SELECT * FROM %s LIMIT 0;", quoted_table, quoted_view),
    conn = conn
  )
  columns <- names(prepared$data)
  reapply_table_keys(meta, columns, conn)
  db_execute(
    sprintf("INSERT INTO %s SELECT * FROM %s;", quoted_table, quoted_view),
    conn = conn
  )
  store_column_labels(table_name, prepared$labels, conn)
  reapply_table_comments(meta, columns, names(prepared$labels), conn)

  if (own_txn) {
    DBI::dbExecute(conn, "COMMIT;")
    committed <- TRUE
  }

  # Table-scoped options can only go on a committed table
  reapply_table_options(meta, conn)

  if (!.quiet) {
    cli::cli_inform("Table {.val {table_name}} successfully replaced.")
  }

  invisible(NULL)
}
