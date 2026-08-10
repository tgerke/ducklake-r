#' Update rows in a DuckLake table
#'
#' A wrapper around dplyr::rows_update() with in_place = TRUE as the default,
#' since DuckLake is designed for in-place modifications.
#'
#' @param x Target table (from get_ducklake_table())
#' @param y Data frame with updates
#' @param by Column(s) to match on
#' @param copy Whether to copy y to the same source as x (default TRUE)
#' @param in_place Whether to modify the table in place (default TRUE for DuckLake)
#' @param unmatched How to handle unmatched rows (default "ignore")
#' @param ... Additional arguments passed to dplyr::rows_update()
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
#' - For bulk transformations that touch most rows, use [replace_table()]. It
#'   collects the transformed data into R and rewrites the whole table --
#'   heavier than the row operations, and it resets the row lineage that the
#'   in-place operations preserve in the change feed.
#'
#' @returns The updated table
#' @family row operations
#' @export
#'
#' @examples
#' \dontrun{
#' # Update rows - in_place = TRUE by default
#' rows_update(
#'   get_ducklake_table("my_table"),
#'   data.frame(id = 1, value = "new"),
#'   by = "id"
#' )
#' }
rows_update <- function(x, y, by = NULL, copy = TRUE, in_place = TRUE, unmatched = "ignore", ...) {
  dplyr::rows_update(x = x, y = y, by = by, copy = copy, in_place = in_place, unmatched = unmatched, ...)
}

#' Prepare the `y` argument for a rows_* operation
#'
#' Local data frames are converted to an inline query on the same connection
#' as `x` via [dbplyr::copy_inline()]. Unlike dplyr's `copy = TRUE` path,
#' this creates no temporary table and starts no transaction of its own, so
#' rows_* calls work inside [with_transaction()] (DuckDB does not support
#' nested transactions).
#'
#' @param x Target lazy table
#' @param y Data frame or lazy table
#' @keywords internal
prep_rows_y <- function(x, y) {
  if (is.data.frame(y)) {
    dbplyr::copy_inline(dbplyr::remote_con(x), y)
  } else {
    y
  }
}

#' @exportS3Method dplyr::rows_update
rows_update.tbl_ducklake <- function(x, y, by = NULL, ...,
                                     unmatched = "ignore",
                                     copy = TRUE, in_place = TRUE) {
  class(x) <- setdiff(class(x), "tbl_ducklake")
  y <- prep_rows_y(x, y)
  dplyr::rows_update(
    x = x, y = y, by = by, ...,
    unmatched = unmatched, copy = copy, in_place = in_place
  )
}

#' Insert rows into a DuckLake table
#'
#' A wrapper around dplyr::rows_insert() with in_place = TRUE as the default,
#' since DuckLake is designed for in-place modifications.
#'
#' @param x Target table (from get_ducklake_table())
#' @param y Data frame with new rows
#' @param by Column(s) to match on (for conflict detection)
#' @param copy Whether to copy y to the same source as x (default TRUE)
#' @param in_place Whether to modify the table in place (default TRUE for DuckLake)
#' @param conflict How to handle conflicts (default "ignore")
#' @param ... Additional arguments passed to dplyr::rows_insert()
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
#' - For bulk transformations that touch most rows, use [replace_table()]. It
#'   collects the transformed data into R and rewrites the whole table --
#'   heavier than the row operations, and it resets the row lineage that the
#'   in-place operations preserve in the change feed.
#'
#' @returns The updated table
#' @family row operations
#' @export
#'
#' @examples
#' \dontrun{
#' rows_insert(
#'   get_ducklake_table("my_table"),
#'   data.frame(id = 99, value = "new row"),
#'   by = "id"
#' )
#' }
rows_insert <- function(x, y, by = NULL, copy = TRUE, in_place = TRUE, conflict = "ignore", ...) {
  dplyr::rows_insert(x = x, y = y, by = by, copy = copy, in_place = in_place, conflict = conflict, ...)
}

#' @exportS3Method dplyr::rows_insert
rows_insert.tbl_ducklake <- function(x, y, by = NULL, ...,
                                     conflict = "ignore",
                                     copy = TRUE, in_place = TRUE) {
  class(x) <- setdiff(class(x), "tbl_ducklake")
  y <- prep_rows_y(x, y)
  dplyr::rows_insert(
    x = x, y = y, by = by, ...,
    conflict = conflict, copy = copy, in_place = in_place
  )
}

#' Delete rows from a DuckLake table
#'
#' A wrapper around dplyr::rows_delete() with in_place = TRUE as the default,
#' since DuckLake is designed for in-place modifications.
#'
#' @param x Target table (from get_ducklake_table())
#' @param y Data frame with rows to delete (matched by 'by' columns)
#' @param by Column(s) to match on
#' @param copy Whether to copy y to the same source as x (default TRUE)
#' @param in_place Whether to modify the table in place (default TRUE for DuckLake)
#' @param unmatched How to handle unmatched rows (default "ignore")
#' @param ... Additional arguments passed to dplyr::rows_delete()
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
#' - For bulk transformations that touch most rows, use [replace_table()]. It
#'   collects the transformed data into R and rewrites the whole table --
#'   heavier than the row operations, and it resets the row lineage that the
#'   in-place operations preserve in the change feed.
#'
#' @returns The updated table
#' @family row operations
#' @export
#'
#' @examples
#' \dontrun{
#' rows_delete(
#'   get_ducklake_table("my_table"),
#'   data.frame(id = c(1, 2, 3)),
#'   by = "id"
#' )
#' }
rows_delete <- function(x, y, by = NULL, copy = TRUE, in_place = TRUE, unmatched = "ignore", ...) {
  dplyr::rows_delete(x = x, y = y, by = by, copy = copy, in_place = in_place, unmatched = unmatched, ...)
}

#' @exportS3Method dplyr::rows_delete
rows_delete.tbl_ducklake <- function(x, y, by = NULL, ...,
                                     unmatched = "ignore",
                                     copy = TRUE, in_place = TRUE) {
  class(x) <- setdiff(class(x), "tbl_ducklake")
  y <- prep_rows_y(x, y)
  dplyr::rows_delete(
    x = x, y = y, by = by, ...,
    unmatched = unmatched, copy = copy, in_place = in_place
  )
}

#' Upsert rows into a DuckLake table
#'
#' Updates rows of `x` that match a row of `y` (by the `by` columns) and
#' inserts the rows of `y` that have no match, as one atomic `MERGE INTO`
#' statement -- a single snapshot, with row lineage preserved in the change
#' feed. A wrapper around dplyr::rows_upsert() with in_place = TRUE as the
#' default, since DuckLake is designed for in-place modifications.
#'
#' @param x Target table (from get_ducklake_table())
#' @param y Data frame with rows to update or insert
#' @param by Column(s) to match on. Defaults to the first column of `y`,
#'   with a message.
#' @param copy Whether to copy y to the same source as x (default TRUE)
#' @param in_place Whether to modify the table in place (default TRUE for DuckLake)
#' @param ... Additional arguments passed to dplyr::rows_upsert()
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
#' - For bulk transformations that touch most rows, use [replace_table()]. It
#'   collects the transformed data into R and rewrites the whole table --
#'   heavier than the row operations, and it resets the row lineage that the
#'   in-place operations preserve in the change feed.
#'
#' ## DuckLake-specific behavior
#'
#' DuckLake tables have no primary keys or unique constraints, so the usual
#' database upsert (`INSERT ... ON CONFLICT`) does not apply. This method
#' instead generates `MERGE INTO`, matching on the `by` columns:
#'
#' - When `y` covers a subset of `x`'s columns, matched rows are updated in
#'   those columns only; inserted rows receive the column's default value in
#'   the remaining columns (`NULL` when the table defines none). Note the
#'   difference from data-frame upserts, which fill with `NA`.
#' - When `y` has only the `by` columns, there is nothing to update and the
#'   call inserts the unmatched rows.
#' - Rows where a `by` column is `NULL` never match and are always inserted.
#' - When several rows of `y` share the same `by` key, each matched update
#'   applies in an unspecified order; the last write wins. Keep `y` keys
#'   unique.
#'
#' @returns The updated table, invisibly
#' @family row operations
#' @export
#'
#' @seealso [merge_into()] for conditional merge clauses and source-driven
#'   deletes.
#'
#' @examples
#' \dontrun{
#' # Update id 2, insert id 4 - one statement, one snapshot
#' rows_upsert(
#'   get_ducklake_table("my_table"),
#'   data.frame(id = c(2, 4), value = c("updated", "new")),
#'   by = "id"
#' )
#' }
rows_upsert <- function(x, y, by = NULL, copy = TRUE, in_place = TRUE, ...) {
  dplyr::rows_upsert(x = x, y = y, by = by, copy = copy, in_place = in_place, ...)
}

#' @exportS3Method dplyr::rows_upsert
rows_upsert.tbl_ducklake <- function(x, y, by = NULL, ...,
                                     copy = TRUE, in_place = TRUE) {
  class(x) <- setdiff(class(x), "tbl_ducklake")
  y <- prep_rows_y(x, y)

  if (!isTRUE(in_place)) {
    return(dplyr::rows_upsert(x = x, y = y, by = by, ..., copy = copy, in_place = FALSE))
  }

  if (...length() > 0) {
    cli::cli_abort(c(
      "Arguments in {.arg ...} are not supported for in-place DuckLake upserts.",
      "i" = "DuckLake's MERGE INTO does not implement RETURNING or other extensions."
    ))
  }

  target <- merge_target_name(x)
  conn <- dbplyr::remote_con(x)

  x_cols <- colnames(x)
  y_cols <- colnames(y)
  extra <- setdiff(y_cols, x_cols)
  if (length(extra) > 0) {
    cli::cli_abort(c(
      "All columns in {.arg y} must exist in {.arg x}.",
      "x" = "Extra column{?s}: {.val {extra}}."
    ))
  }

  if (is.null(by)) {
    by <- y_cols[[1]]
    cli::cli_inform('Matching, by = "{by}"')
  }
  if (!is.character(by) || length(by) == 0 || anyNA(by)) {
    cli::cli_abort("{.arg by} must be a character vector of column names.")
  }
  missing_by <- setdiff(by, intersect(x_cols, y_cols))
  if (length(missing_by) > 0) {
    cli::cli_abort(c(
      "All {.arg by} columns must exist in both {.arg x} and {.arg y}.",
      "x" = "Missing: {.val {missing_by}}."
    ))
  }

  update_cols <- setdiff(y_cols, by)
  clauses <- character(0)
  if (length(update_cols) > 0) {
    clauses <- c(clauses, sprintf(
      "WHEN MATCHED THEN UPDATE SET %s",
      merge_update_set_sql(update_cols, conn, "dl_target_source")
    ))
  }
  clauses <- c(clauses, "WHEN NOT MATCHED THEN INSERT BY NAME")

  sql <- build_merge_sql(
    target_sql = quote_ident(target, conn),
    source_sql = dbplyr::sql_render(y, conn, lvl = 1),
    on_sql = merge_on_sql(by, conn, "dl_target", "dl_target_source"),
    clauses = clauses,
    target_alias = "dl_target",
    source_alias = "dl_target_source"
  )
  db_execute(sql, conn = conn)

  invisible(x)
}

#' Resolve the physical table behind a merge target
#'
#' MERGE modifies a stored table, so the input must be an unmodified
#' reference from [get_ducklake_table()], not a pipeline with pending verbs.
#'
#' @param x A lazy table.
#' @param arg Argument name for error messages.
#' @returns The table name as a string.
#' @noRd
merge_target_name <- function(x, arg = "x") {
  remote <- tryCatch(dbplyr::remote_name(x), error = function(e) NULL)
  if (is.null(remote)) {
    cli::cli_abort(c(
      "{.arg {arg}} must be an unmodified table reference from {.fun get_ducklake_table}.",
      "i" = "A pipeline with pending verbs has no stored table to merge into."
    ))
  }
  name <- attr(x, "ducklake_table_name")
  if (is.null(name)) {
    name <- as.character(remote)
  }
  name
}

#' Assemble a MERGE INTO statement from rendered pieces
#'
#' @param target_sql Quoted target table identifier.
#' @param source_sql Rendered SQL for the USING subquery.
#' @param on_sql Rendered ON condition.
#' @param clauses Character vector of WHEN ... clauses, in order.
#' @param target_alias,source_alias Statement aliases.
#' @returns A single MERGE INTO statement string.
#' @noRd
build_merge_sql <- function(target_sql, source_sql, on_sql, clauses,
                            target_alias, source_alias) {
  sprintf(
    "MERGE INTO %s AS %s USING (\n%s\n) AS %s ON %s\n%s",
    target_sql, target_alias, source_sql, source_alias, on_sql,
    paste(clauses, collapse = "\n")
  )
}

#' Render the ON equality condition for a MERGE
#'
#' Plain `=` comparisons: rows with NULL key values never match, consistent
#' with dbplyr's `na_matches = "never"` behavior for rows_* operations.
#'
#' @param by Character vector of key columns.
#' @param conn Connection used for identifier quoting.
#' @param target_alias,source_alias Statement aliases.
#' @returns The ON condition as a string.
#' @noRd
merge_on_sql <- function(by, conn, target_alias, source_alias) {
  paste(
    vapply(
      by,
      function(k) {
        qk <- as.character(DBI::dbQuoteIdentifier(conn, k))
        sprintf("%s.%s = %s.%s", target_alias, qk, source_alias, qk)
      },
      character(1)
    ),
    collapse = " AND "
  )
}

#' Render the UPDATE SET list for a MERGE
#'
#' @param cols Columns to update.
#' @param conn Connection used for identifier quoting.
#' @param source_alias Statement alias of the merge source.
#' @returns The SET list as a string.
#' @noRd
merge_update_set_sql <- function(cols, conn, source_alias) {
  paste(
    vapply(
      cols,
      function(col) {
        qc <- as.character(DBI::dbQuoteIdentifier(conn, col))
        sprintf("%s = %s.%s", qc, source_alias, qc)
      },
      character(1)
    ),
    collapse = ", "
  )
}
