#' Merge a source table into a DuckLake table
#'
#' Runs a SQL `MERGE INTO` statement: rows of `target` are matched against
#' rows of `source` on the `by` columns, then updated, deleted, or left
#' alone, while unmatched source rows can be inserted and target rows
#' missing from the source can be removed. The whole operation is atomic --
#' one snapshot, with row lineage preserved in the change feed.
#'
#' This is not a join. Joins (`left_join()` and friends) read from the lake
#' and build a new result without touching either table; `merge_into()`
#' changes the rows of `target` in place. For the common update-or-insert
#' case, reach for [rows_upsert()] first -- `merge_into()` is for the cases
#' it cannot express: conditional clauses, deletes of matched rows, and
#' synchronizing a table with a staging source.
#'
#' @param target The table to modify: a table from [get_ducklake_table()] or
#'   a table name.
#' @param source The rows to merge in: a data frame or a lazy table on the
#'   same connection.
#' @param by Character vector of key column(s) to match on. Rows with `NULL`
#'   key values never match.
#' @param when_matched What to do with target rows that match a source row:
#'   `"update"` (default) sets the columns the two tables share, `"delete"`
#'   removes the row, `"nothing"` leaves it alone.
#' @param when_not_matched What to do with source rows that match no target
#'   row: `"insert"` (default) adds them, `"nothing"` skips them.
#' @param matched_condition Optional SQL expression further restricting the
#'   `when_matched` action, written against the aliases `target` and
#'   `source`, e.g. `"source.amt > target.amt"`.
#' @param not_matched_condition Optional SQL expression further restricting
#'   the `when_not_matched` action.
#' @param delete_missing Also delete target rows that have no match in the
#'   source (`WHEN NOT MATCHED BY SOURCE THEN DELETE`). Combined with the
#'   update action this synchronizes `target` to `source`.
#' @param .quiet Logical, whether to suppress the row-count message
#'   (default TRUE).
#'
#' @details
#' ## Choosing how to change a table
#'
#' - To look up or combine data for analysis, use dplyr joins
#'   (`left_join()` and friends). Joins read from the lake and build a new
#'   result; they never modify a lake table.
#' - To append, correct, or remove specific rows, use [rows_insert()],
#'   [rows_update()], or [rows_delete()]. Each call is a single SQL statement
#'   against the existing table -- no data leaves the database, and with data
#'   inlining enabled (DuckLake's default) small changes land in the catalog
#'   without creating tiny Parquet files.
#' - To update rows that exist and insert the ones that don't in one atomic
#'   statement, use [rows_upsert()].
#' - For conditional merge logic or deletes driven by a staging table, use
#'   [merge_into()].
#' - To change a table's shape without touching its data -- add, drop, or
#'   rename columns, widen a type -- use the schema evolution family
#'   ([add_table_column()] and friends): metadata-only changes that rewrite
#'   nothing.
#' - For bulk transformations that touch most rows, use [replace_table()]. It
#'   collects the transformed data into R and rewrites the whole table --
#'   heavier than the row operations, and it resets the row lineage that the
#'   in-place operations preserve in the change feed.
#'
#' A tempting alternative -- joining the source to the table and calling
#' [replace_table()] on the result -- rewrites every row and records the
#' change as a wholesale replacement. `merge_into()` touches only the
#' affected rows, so [get_table_changes()] afterward shows exactly which
#' rows were inserted, updated, or deleted.
#'
#' ## Conditions are SQL
#'
#' `matched_condition` and `not_matched_condition` are raw SQL expressions,
#' not dplyr code. They are pasted into the statement as written (only a
#' `;` is rejected), so build them from trusted input only.
#'
#' ## DuckLake-specific behavior
#'
#' - Update sets the columns present in both tables (minus `by`); inserted
#'   rows receive the column default (`NULL` when none is defined) in target
#'   columns the source lacks.
#' - `MERGE ... RETURNING` is not implemented by DuckLake.
#' - DuckLake currently supports one update/delete action per MERGE
#'   statement. `delete_missing = TRUE` together with a `when_matched`
#'   action therefore runs as a MERGE plus a `DELETE` of the unmatched rows,
#'   wrapped in one transaction (a single snapshot). When you already opened
#'   a transaction, both statements simply join it.
#' - When several source rows share a `by` key, matched updates apply in an
#'   unspecified order; keep source keys unique.
#'
#' @returns The number of affected rows, invisibly.
#' @family row operations
#' @export
#'
#' @seealso [rows_upsert()] for the plain update-or-insert case;
#'   [get_table_changes()] to inspect what a merge did.
#'
#' @examples
#' \dontrun{
#' # Update only when the source amount is higher; insert new ids
#' merge_into(
#'   get_ducklake_table("sales"), new_sales, by = "sls_id",
#'   matched_condition = "source.sls_amt > target.sls_amt"
#' )
#'
#' # Synchronize to a staging table: upsert + drop rows gone from the source
#' merge_into(
#'   "sales", staging_sales, by = "sls_id",
#'   delete_missing = TRUE
#' )
#'
#' # Remove rows flagged in the source
#' merge_into(
#'   "sales", withdrawn, by = "sls_id",
#'   when_matched = "delete", when_not_matched = "nothing"
#' )
#' }
merge_into <- function(target, source, by,
                       when_matched = c("update", "delete", "nothing"),
                       when_not_matched = c("insert", "nothing"),
                       matched_condition = NULL,
                       not_matched_condition = NULL,
                       delete_missing = FALSE,
                       .quiet = TRUE) {
  when_matched <- match.arg(when_matched)
  when_not_matched <- match.arg(when_not_matched)

  if (is.character(target) && length(target) == 1 && !is.na(target)) {
    conn <- get_ducklake_connection()
    target_name <- target
    target_tbl <- dplyr::tbl(conn, target)
  } else if (inherits(target, "tbl_lazy")) {
    conn <- dbplyr::remote_con(target)
    target_name <- merge_target_name(target, arg = "target")
    target_tbl <- target
  } else {
    cli::cli_abort(
      "{.arg target} must be a table name or a table from {.fun get_ducklake_table}."
    )
  }

  if (is.data.frame(source)) {
    source_tbl <- dbplyr::copy_inline(conn, source)
  } else if (inherits(source, "tbl_lazy")) {
    source_tbl <- source
  } else {
    cli::cli_abort("{.arg source} must be a data frame or a lazy table.")
  }

  check_merge_condition(matched_condition, "matched_condition")
  check_merge_condition(not_matched_condition, "not_matched_condition")
  if (when_matched == "nothing" && !is.null(matched_condition)) {
    cli::cli_abort(
      "{.arg matched_condition} has no effect when {.arg when_matched} is {.val nothing}."
    )
  }
  if (when_not_matched == "nothing" && !is.null(not_matched_condition)) {
    cli::cli_abort(
      "{.arg not_matched_condition} has no effect when {.arg when_not_matched} is {.val nothing}."
    )
  }
  if (when_matched == "nothing" && when_not_matched == "nothing" &&
      !isTRUE(delete_missing)) {
    cli::cli_abort(
      "Nothing to do: both actions are {.val nothing} and {.arg delete_missing} is FALSE."
    )
  }

  target_cols <- colnames(target_tbl)
  source_cols <- colnames(source_tbl)

  if (!is.character(by) || length(by) == 0 || anyNA(by)) {
    cli::cli_abort("{.arg by} must be a character vector of column names.")
  }
  missing_by <- setdiff(by, intersect(target_cols, source_cols))
  if (length(missing_by) > 0) {
    cli::cli_abort(c(
      "All {.arg by} columns must exist in both {.arg target} and {.arg source}.",
      "x" = "Missing: {.val {missing_by}}."
    ))
  }

  if (when_not_matched == "insert") {
    extra <- setdiff(source_cols, target_cols)
    if (length(extra) > 0) {
      cli::cli_abort(c(
        "All {.arg source} columns must exist in {.arg target} to insert by name.",
        "x" = "Extra column{?s}: {.val {extra}}.",
        "i" = "{cli::qty(extra)}Drop {?it/them} from the source or use {.code when_not_matched = \"nothing\"}."
      ))
    }
  }

  update_cols <- setdiff(intersect(source_cols, target_cols), by)
  if (when_matched == "update" && length(update_cols) == 0) {
    cli::cli_abort(c(
      "{.arg source} has no non-key columns shared with {.arg target} to update.",
      "i" = "Use {.code when_matched = \"nothing\"} or {.code \"delete\"}."
    ))
  }

  # DuckLake allows a single update/delete action per MERGE statement, so
  # delete_missing alongside a matched action runs as MERGE + DELETE in one
  # transaction (one snapshot).
  needs_fallback <- isTRUE(delete_missing) && when_matched != "nothing"

  matched_and <- if (is.null(matched_condition)) "" else {
    sprintf(" AND (%s)", matched_condition)
  }
  not_matched_and <- if (is.null(not_matched_condition)) "" else {
    sprintf(" AND (%s)", not_matched_condition)
  }

  clauses <- character(0)
  if (when_matched == "update") {
    clauses <- c(clauses, sprintf(
      "WHEN MATCHED%s THEN UPDATE SET %s",
      matched_and, merge_update_set_sql(update_cols, conn, "source")
    ))
  } else if (when_matched == "delete") {
    clauses <- c(clauses, sprintf("WHEN MATCHED%s THEN DELETE", matched_and))
  }
  if (when_not_matched == "insert") {
    clauses <- c(clauses, sprintf(
      "WHEN NOT MATCHED%s THEN INSERT BY NAME", not_matched_and
    ))
  }
  if (isTRUE(delete_missing) && !needs_fallback) {
    clauses <- c(clauses, "WHEN NOT MATCHED BY SOURCE THEN DELETE")
  }

  target_sql <- quote_ident(target_name, conn)
  source_sql <- dbplyr::sql_render(source_tbl, conn, lvl = 1)

  statements <- character(0)
  if (length(clauses) > 0) {
    statements <- c(statements, build_merge_sql(
      target_sql = target_sql,
      source_sql = source_sql,
      on_sql = merge_on_sql(by, conn, "target", "source"),
      clauses = clauses,
      target_alias = "target",
      source_alias = "source"
    ))
  }
  if (needs_fallback) {
    statements <- c(statements, sprintf(
      "DELETE FROM %s WHERE NOT EXISTS (\nSELECT 1 FROM (\n%s\n) AS source WHERE %s\n)",
      target_sql, source_sql,
      merge_on_sql(by, conn, target_sql, "source")
    ))
  }

  own_txn <- length(statements) > 1 && !in_transaction(conn)
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
  n <- 0
  for (statement in statements) {
    n <- n + DBI::dbExecute(conn, statement)
  }
  if (own_txn) {
    DBI::dbExecute(conn, "COMMIT;")
    committed <- TRUE
  }

  if (!.quiet) {
    cli::cli_inform(
      "Merged {.arg source} into {.val {target_name}}: {n} row{?s} affected."
    )
  }

  invisible(n)
}

#' Validate a raw SQL condition for merge_into()
#'
#' @param cond The condition string, or NULL.
#' @param arg Argument name for error messages.
#' @returns NULL, invisibly.
#' @noRd
check_merge_condition <- function(cond, arg) {
  if (is.null(cond)) {
    return(invisible(NULL))
  }
  if (!is.character(cond) || length(cond) != 1 || is.na(cond) || !nzchar(cond)) {
    cli::cli_abort("{.arg {arg}} must be a single SQL expression.")
  }
  if (grepl(";", cond, fixed = TRUE)) {
    cli::cli_abort(
      "{.arg {arg}} must be a single SQL expression, without {.val ;}."
    )
  }
  invisible(NULL)
}
