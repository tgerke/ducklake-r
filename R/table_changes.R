#' Get the changes made to a table between two snapshots
#'
#' Returns the exact rows that were inserted, deleted, or updated in a table
#' between two snapshots (inclusive), using DuckLake's data change feed.
#' Useful for auditing and for change-data-capture style pipelines.
#'
#' @param table_name The name of the table to inspect, optionally
#'   qualified as `"schema.table"` (default schema `main`).
#' @param start The first snapshot to include: either a snapshot id (see
#'   [list_table_snapshots()]) or a timestamp (POSIXct or character). With
#'   `end` also `NULL` (the default), the feed covers the table's full
#'   history, from its first snapshot to its latest.
#' @param end The last snapshot to include, in the same form as `start`.
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#' @param conn Optional DuckDB connection object. If not provided, uses the
#'   default ducklake connection.
#'
#' @returns A dplyr lazy query object (tbl_lazy). In addition to the table's
#'   own columns it carries `snapshot_id` (the snapshot that made the
#'   change), `rowid` (the changed row's identifier), and `change_type`
#'   (`"insert"`, `"delete"`, `"update_preimage"`, or
#'   `"update_postimage"`). It also carries a `ducklake_changes` attribute
#'   naming the table, the lake, and the requested range, which
#'   [view_table_changes()] reads; the attribute survives dplyr verbs on the
#'   lazy table and is dropped by `collect()`.
#' @family time travel
#' @export
#'
#' @details
#' Both bounds must be given or both left `NULL`. When given, they must be
#' of the same kind: two snapshot ids or two timestamps. POSIXct bounds are
#' converted to UTC, matching the snapshot times DuckLake records; character
#' bounds are passed through as-is and must already be in UTC. Bounds before
#' the lake's first snapshot are rejected by DuckLake, so prefer snapshot
#' times from [list_table_snapshots()]. Updates appear as two rows -- the
#' row as it looked before the change (`update_preimage`) and after it
#' (`update_postimage`).
#'
#' The feed is read with the table's schema as of the end snapshot: a column
#' added inside the range is `NA` in rows from before it existed, and a
#' renamed column appears under its newer name. A table rebuilt by
#' [replace_table()] or [restore_table_version()] gets a new table id, and
#' the feed covers the current id only, so changes from before the rebuild
#' are not in it.
#'
#' This wraps DuckLake's
#' [`table_changes()`](https://ducklake.select/docs/stable/duckdb/advanced_features/data_change_feed)
#' function.
#'
#' @seealso [view_table_changes()] to browse the feed interactively,
#'   [plot_table_changes()], [list_table_snapshots()],
#'   [get_ducklake_table_version()], [get_ducklake_table_asof()]
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("changes_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("changes_lake", lake_path = lake_dir)
#' create_table(data.frame(id = 1:3, amount = c(10, 20, 30)), "orders")
#'
#' rows_delete(
#'   get_ducklake_table("orders"),
#'   data.frame(id = 1L),
#'   by = "id"
#' )
#' snaps <- list_table_snapshots("orders")
#'
#' # Everything that ever happened to the table
#' get_table_changes("orders") |> dplyr::collect()
#'
#' # What changed in the most recent snapshot?
#' latest <- max(snaps$snapshot_id)
#' get_table_changes("orders", latest, latest) |> dplyr::collect()
#'
#' # Every change across the table's full history, by timestamp
#' get_table_changes(
#'   "orders",
#'   min(snaps$snapshot_time), max(snaps$snapshot_time) + 1
#' ) |>
#'   dplyr::filter(change_type == "delete") |>
#'   dplyr::collect()
#'
#' detach_ducklake("changes_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
get_table_changes <- function(table_name, start = NULL, end = NULL,
                              ducklake_name = NULL, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  if (is.null(start) != is.null(end)) {
    cli::cli_abort(
      "Give both {.arg start} and {.arg end}, or neither for the table's full history."
    )
  }
  if (is.null(start)) {
    snapshots <- list_table_snapshots(table_name, ducklake_name, conn)
    if (nrow(snapshots) == 0) {
      cli::cli_abort(c(
        "No snapshots found for {.val {table_name}}.",
        "i" = "Make sure the ducklake is attached and has snapshots."
      ))
    }
    start <- min(snapshots$snapshot_id)
    end <- max(snapshots$snapshot_id)
  }

  is_version <- function(x) is.numeric(x) && !inherits(x, "POSIXct")
  is_time <- function(x) inherits(x, "POSIXct") || is.character(x)

  if (is_version(start) && is_version(end)) {
    bound_type <- "snapshot"
    bounds <- sprintf("%d, %d", as.integer(start), as.integer(end))
  } else if (is_time(start) && is_time(end)) {
    bound_type <- "timestamp"
    bounds <- sprintf(
      "TIMESTAMP %s, TIMESTAMP %s",
      quote_sql(format_timestamp(start)),
      quote_sql(format_timestamp(end))
    )
  } else {
    cli::cli_abort(
      "{.arg start} and {.arg end} must both be snapshot ids or both be timestamps."
    )
  }

  ref <- resolve_table_ref(table_name)
  schema <- if (is.null(ref$schema)) "main" else ref$schema
  query <- sprintf(
    "SELECT * FROM ducklake_table_changes(%s, %s, %s, %s)",
    quote_sql(ducklake_name), quote_sql(schema), quote_sql(ref$table), bounds
  )

  changes <- dplyr::tbl(conn, dplyr::sql(query))
  # Read by view_table_changes(). dplyr verbs keep the attribute because they
  # assign into the same object; collect() returns a fresh tibble without it.
  attr(changes, "ducklake_changes") <- list(
    table_name = paste0(schema, ".", ref$table),
    schema = schema,
    table = ref$table,
    ducklake_name = ducklake_name,
    bound_type = bound_type,
    start = start,
    end = end
  )
  changes
}
