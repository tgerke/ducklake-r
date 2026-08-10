#' Set the comment on a DuckLake table
#'
#' Stores a description of the table in the DuckLake catalog with
#' `COMMENT ON TABLE`. Comments live in the lake itself, so every client --
#' R, Python, or plain SQL -- sees the same documentation, and AI tools
#' reading the catalog get the context too.
#'
#' @param table_name The table to describe.
#' @param comment The comment text, or `NULL`/`NA` to clear an existing
#'   comment.
#'
#' @returns Invisibly returns `NULL`.
#' @family table documentation
#' @export
#'
#' @seealso [set_column_comments()], [get_table_comments()]
#'
#' @examples
#' \dontrun{
#' set_table_comment("adsl", "Subject-level analysis dataset, one row per subject")
#' set_table_comment("scratch", NULL)  # clear
#' }
set_table_comment <- function(table_name, comment) {
  conn <- get_ducklake_connection()

  db_execute(
    sprintf(
      "COMMENT ON TABLE %s IS %s;",
      quote_ident(table_name, conn),
      render_comment_value(comment)
    ),
    conn = conn
  )
  if (is_empty_comment(comment)) {
    cli::cli_inform("Cleared the comment on {.val {table_name}}.")
  } else {
    cli::cli_inform("Commented table {.val {table_name}}.")
  }

  invisible(NULL)
}

#' Set column comments on a DuckLake table
#'
#' Stores per-column descriptions in the DuckLake catalog with
#' `COMMENT ON COLUMN`, one `name = "comment"` pair per column. For data
#' with haven/labelled variable labels, [create_table()] can store the
#' labels automatically; this function adds or revises them afterward --
#' for example to label a derived variable.
#'
#' @param table_name The table whose columns to describe.
#' @param ... Named comments, e.g. `USUBJID = "Unique subject identifier"`.
#'   Use `NA` (or `NULL`) as a value to clear that column's comment. To
#'   pass a named vector built elsewhere, splice it with
#'   `do.call(set_column_comments, c(list("tbl"), as.list(my_labels)))`.
#'
#' @details
#' All comments from one call are written in a single transaction, so they
#' land as one snapshot. Inside [with_transaction()] they join the open
#' transaction instead.
#'
#' @returns Invisibly returns `NULL`.
#' @family table documentation
#' @export
#'
#' @seealso [set_table_comment()], [get_table_comments()]
#'
#' @examples
#' \dontrun{
#' set_column_comments(
#'   "adsl",
#'   USUBJID = "Unique subject identifier",
#'   AGEGR1 = "Age group 1",
#'   SCRATCH = NA  # clear this one
#' )
#' }
set_column_comments <- function(table_name, ...) {
  conn <- get_ducklake_connection()

  comments <- list(...)
  if (length(comments) == 0) {
    cli::cli_abort("Provide at least one {.code column = \"comment\"} pair.")
  }
  if (is.null(names(comments)) || any(!nzchar(names(comments)))) {
    cli::cli_abort(
      "Every argument in {.arg ...} must be named after a column."
    )
  }

  statements <- vapply(
    names(comments),
    function(col) {
      sprintf(
        "COMMENT ON COLUMN %s.%s IS %s;",
        quote_ident(table_name, conn),
        quote_column(col, conn),
        render_comment_value(comments[[col]])
      )
    },
    character(1)
  )

  # One call = one snapshot: wrap multiple statements in a transaction of
  # our own unless the caller already opened one
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
  for (statement in statements) {
    db_execute(statement, conn = conn)
  }
  if (own_txn) {
    DBI::dbExecute(conn, "COMMIT;")
    committed <- TRUE
  }

  cli::cli_inform(
    "Commented {length(statements)} column{?s} on {.val {table_name}}."
  )

  invisible(NULL)
}

#' Read table, view, and column comments from a DuckLake catalog
#'
#' Returns the current comments stored in the lake -- via
#' [set_table_comment()], [set_column_comments()], [create_table()]'s label
#' sync, or any other client -- as a tidy data frame.
#'
#' @param table_name Optional table (or view) name to filter to.
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @returns A data frame with one row per comment: `object_type`
#'   (`"table"`, `"view"`, or `"column"`), `table_name`, `column_name`
#'   (`NA` for tables and views), and `comment`. Zero rows when nothing is
#'   commented.
#' @family table documentation
#' @export
#'
#' @seealso [get_metadata_table()] for the raw `ducklake_tag` and
#'   `ducklake_column_tag` catalog tables.
#'
#' @examples
#' \dontrun{
#' # Everything documented in the lake
#' get_table_comments()
#'
#' # One table's documentation
#' get_table_comments("adsl")
#' }
get_table_comments <- function(table_name = NULL, ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  # Metadata tables live in the __ducklake_metadata_[name] database.
  # DuckDB and SQLite use a .main. schema qualifier; PostgreSQL and MySQL do not.
  meta_db <- paste0("__ducklake_metadata_", ducklake_name)
  prefix <- if (get_ducklake_backend() %in% c("postgres", "mysql")) {
    quote_ident(meta_db, conn)
  } else {
    paste0(quote_ident(meta_db, conn), ".main")
  }

  filter_table <- if (is.null(table_name)) "" else "AND t.table_name = ?"
  filter_view <- if (is.null(table_name)) "" else "AND v.view_name = ?"

  # Cleared comments are NULL-valued rows; current rows have
  # end_snapshot IS NULL
  sql <- sprintf(
    "SELECT 'table' AS object_type, t.table_name, NULL AS column_name,
            tag.value AS comment
     FROM %s.ducklake_tag tag
     JOIN %s.ducklake_table t
       ON tag.object_id = t.table_id AND t.end_snapshot IS NULL
     WHERE tag.end_snapshot IS NULL AND tag.key = 'comment'
       AND tag.value IS NOT NULL %s
     UNION ALL
     SELECT 'view', v.view_name, NULL, tag.value
     FROM %s.ducklake_tag tag
     JOIN %s.ducklake_view v
       ON tag.object_id = v.view_id AND v.end_snapshot IS NULL
     WHERE tag.end_snapshot IS NULL AND tag.key = 'comment'
       AND tag.value IS NOT NULL %s
     UNION ALL
     SELECT 'column', t.table_name, c.column_name, ct.value
     FROM %s.ducklake_column_tag ct
     JOIN %s.ducklake_table t
       ON ct.table_id = t.table_id AND t.end_snapshot IS NULL
     JOIN %s.ducklake_column c
       ON ct.table_id = c.table_id AND ct.column_id = c.column_id
       AND c.end_snapshot IS NULL
     WHERE ct.end_snapshot IS NULL AND ct.key = 'comment'
       AND ct.value IS NOT NULL %s
     ORDER BY object_type, table_name, column_name",
    prefix, prefix, filter_table,
    prefix, prefix, filter_view,
    prefix, prefix, prefix, filter_table
  )

  if (is.null(table_name)) {
    DBI::dbGetQuery(conn, sql)
  } else {
    DBI::dbGetQuery(conn, sql, params = list(table_name, table_name, table_name))
  }
}

#' Reattach stored column comments as label attributes on collect
#'
#' Completes the label round trip: [create_table()] stores haven/labelled
#' `label` attributes as column comments, and collecting the table brings
#' them back, so gtsummary, gt, and friends display them as usual. Columns
#' renamed or derived in the pipeline simply come back unlabelled.
#'
#' @param x A `tbl_ducklake` lazy table.
#' @param ... Passed on to [dplyr::collect()].
#' @returns A tibble, with `label` attributes on columns that have stored
#'   comments.
#' @exportS3Method dplyr::collect
#' @keywords internal
collect.tbl_ducklake <- function(x, ...) {
  out <- NextMethod()

  tbl_name <- attr(x, "ducklake_table_name")
  if (is.null(tbl_name)) {
    return(out)
  }
  comments <- tryCatch(
    get_table_comments(tbl_name),
    error = function(e) NULL
  )
  if (is.null(comments) || nrow(comments) == 0) {
    return(out)
  }
  cols <- comments[comments$object_type == "column", , drop = FALSE]
  for (i in seq_len(nrow(cols))) {
    col <- cols$column_name[[i]]
    if (col %in% names(out)) {
      attr(out[[col]], "label") <- cols$comment[[i]]
    }
  }
  out
}

#' Render a comment value for COMMENT ON
#'
#' @param comment The comment text, NULL, or NA.
#' @returns A SQL literal string ("NULL" clears the comment).
#' @noRd
render_comment_value <- function(comment) {
  if (is_empty_comment(comment)) {
    return("NULL")
  }
  if (!is.character(comment) || length(comment) != 1) {
    cli::cli_abort("A comment must be a single string, {.code NULL}, or {.code NA}.")
  }
  quote_sql(comment)
}

#' Is a comment value a clear request?
#'
#' @noRd
is_empty_comment <- function(comment) {
  is.null(comment) || (length(comment) == 1 && is.na(comment))
}
