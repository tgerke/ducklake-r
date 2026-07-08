#' Get the changes made to a table between two snapshots
#'
#' Returns the exact rows that were inserted, deleted, or updated in a table
#' between two snapshots (inclusive), using DuckLake's data change feed.
#' Useful for auditing and for change-data-capture style pipelines.
#'
#' @param table_name The name of the table to inspect.
#' @param start The first snapshot to include: either a snapshot id (see
#'   [list_table_snapshots()]) or a timestamp (POSIXct or character).
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
#'   `"update_postimage"`).
#' @family time travel
#' @export
#'
#' @details
#' Both bounds must be of the same kind: two snapshot ids or two
#' timestamps. POSIXct bounds are converted to UTC, matching the snapshot
#' times DuckLake records; character bounds are passed through as-is and
#' must already be in UTC. Bounds before the lake's first snapshot are
#' rejected by DuckLake, so prefer snapshot times from
#' [list_table_snapshots()]. Updates appear as two rows -- the row as it
#' looked before the change (`update_preimage`) and after it
#' (`update_postimage`).
#'
#' This wraps DuckLake's
#' [`table_changes()`](https://ducklake.select/docs/stable/duckdb/advanced_features/data_change_feed)
#' function.
#'
#' @seealso [list_table_snapshots()], [get_ducklake_table_version()],
#'   [get_ducklake_table_asof()]
#'
#' @examples
#' \dontrun{
#' # What changed in snapshot 3?
#' get_table_changes("orders", 3, 3) |> dplyr::collect()
#'
#' # Every change across the table's full history, by timestamp
#' snaps <- list_table_snapshots("orders")
#' get_table_changes(
#'   "orders",
#'   min(snaps$snapshot_time), max(snaps$snapshot_time) + 1
#' ) |>
#'   dplyr::filter(change_type == "delete") |>
#'   dplyr::collect()
#' }
get_table_changes <- function(table_name, start, end,
                              ducklake_name = NULL, conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  is_version <- function(x) is.numeric(x) && !inherits(x, "POSIXct")
  is_time <- function(x) inherits(x, "POSIXct") || is.character(x)

  if (is_version(start) && is_version(end)) {
    bounds <- sprintf("%d, %d", as.integer(start), as.integer(end))
  } else if (is_time(start) && is_time(end)) {
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

  query <- sprintf(
    "SELECT * FROM %s.table_changes(%s, %s)",
    ducklake_name, quote_sql(table_name), bounds
  )

  dplyr::tbl(conn, dplyr::sql(query))
}
