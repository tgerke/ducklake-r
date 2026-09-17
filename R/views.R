#' Create a DuckLake view from a dplyr pipeline
#'
#' Stores a dplyr pipeline in the lake as a SQL view: the query runs fresh
#' every time the view is read, so it always reflects the current data.
#' Views live in the DuckLake catalog itself, which makes them a good home
#' for shared business logic -- a Python or SQL client of the same lake
#' sees exactly the same definition.
#'
#' @param .data A lazy table (a dplyr pipeline built on
#'   [get_ducklake_table()]). Not a data frame: a view stores a query, not
#'   data -- use [create_table()] to store data.
#' @param view_name Name for the view.
#' @param replace Replace an existing view of the same name (default TRUE).
#'   The replaced view's comment carries over.
#'
#' @details
#' Read a view back with [get_ducklake_table()], which works for views and
#' tables alike, and keep piping dplyr verbs onto it. Like tables, views
#' are versioned: dropping or replacing one is a snapshot like any other.
#'
#' DuckLake stores a replaced view as a new catalog entry and keys the
#' comment to the entry, so `CREATE OR REPLACE VIEW` by itself drops the
#' comment. `create_view()` reads the comment first and sets it again, in
#' the same snapshot as the replacement, the way [replace_table()] carries a
#' table's comments over. Reword it with [set_table_comment()], or clear it
#' with `NULL`.
#'
#' A view kept in a schema of its own should read its tables by
#' schema-qualified name: `get_ducklake_table("main.cars")`, not `"cars"`.
#' DuckDB resolves an unqualified name from the view's schema and the
#' session's current database, so the view can fail to bind in a session
#' where another database is current, and a view that shares its name with
#' the table it reads (`checks.cars` over `cars`) reads itself and fails
#' with a recursion error.
#'
#' @returns Invisibly returns `NULL`.
#' @family table operations
#' @export
#'
#' @seealso [drop_view()], [list_ducklake_tables()],
#'   [replace_table()] to materialize a pipeline as data instead.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("view_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("view_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' # Encapsulate filtering logic the whole team should share
#' get_ducklake_table("cars") |>
#'   dplyr::filter(cyl == 4) |>
#'   dplyr::select(mpg, cyl, gear) |>
#'   create_view("v_efficient_cars")
#'
#' # Reads run the stored query against current data
#' get_ducklake_table("v_efficient_cars") |> dplyr::collect()
#'
#' detach_ducklake("view_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
create_view <- function(.data, view_name, replace = TRUE) {
  if (!inherits(.data, "tbl_lazy")) {
    cli::cli_abort(c(
      "{.arg .data} must be a lazy table, e.g. a pipeline built on {.fun get_ducklake_table}.",
      "i" = "A view stores a query, not data. To store data, use {.fun create_table}.",
      "i" = "To create a view from raw SQL, use {.fun DBI::dbExecute} directly."
    ))
  }

  conn <- dbplyr::remote_con(.data)
  quoted <- quote_ident(view_name, conn)
  create_sql <- sprintf(
    "CREATE %sVIEW %s AS\n%s;",
    if (isTRUE(replace)) "OR REPLACE " else "",
    quoted,
    dbplyr::sql_render(.data, conn)
  )

  if (!isTRUE(replace)) {
    db_execute(create_sql, conn = conn)
  } else {
    # A replaced view gets a new id and DuckLake keys the comment to the id:
    # read it and put it back, in one snapshot with the replacement. Read
    # inside the transaction so both see the same state of the lake.
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
    comment <- find_lake_view(view_name, conn)$comment
    db_execute(create_sql, conn = conn)
    if (length(comment) == 1 && !is.na(comment)) {
      db_execute(
        sprintf("COMMENT ON VIEW %s IS %s;", quoted, quote_sql(comment)),
        conn = conn
      )
    }
    if (own_txn) {
      DBI::dbExecute(conn, "COMMIT;")
      committed <- TRUE
    }
  }
  dl_inform("Created view {.val {view_name}}.")

  invisible(NULL)
}

#' A view's entry in the current lake, from DuckDB's catalog
#'
#' `duckdb_views()` shows a view created in the open transaction and follows
#' a snapshot-pinned attach, which the metadata tables do not.
#'
#' @param view_name A name, optionally qualified as `"schema.view"`.
#' @param conn A DBI connection.
#' @returns A data frame with a `comment` column: one row when the name is a
#'   view, none otherwise.
#' @noRd
find_lake_view <- function(view_name, conn) {
  parts <- split_table_name(view_name)
  DBI::dbGetQuery(
    conn,
    "SELECT comment FROM duckdb_views()
     WHERE database_name = ? AND schema_name = ? AND view_name = ?",
    params = list(
      infer_ducklake_name(NULL, conn),
      if (is.null(parts$schema)) "main" else parts$schema,
      parts$table
    )
  )
}

#' Drop a DuckLake view
#'
#' Removes a view from the lake with `DROP VIEW`. Only the stored query is
#' dropped; the tables it reads are untouched.
#'
#' @param view_name The view to drop.
#'
#' @returns Invisibly returns `NULL`.
#' @family table operations
#' @export
#'
#' @seealso [create_view()]
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("dropview_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("dropview_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' get_ducklake_table("cars") |>
#'   dplyr::filter(cyl == 4) |>
#'   create_view("v_efficient_cars")
#'
#' drop_view("v_efficient_cars")
#'
#' detach_ducklake("dropview_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
drop_view <- function(view_name) {
  conn <- get_ducklake_connection()

  db_execute(
    sprintf("DROP VIEW %s;", quote_ident(view_name, conn)),
    conn = conn
  )
  dl_inform("Dropped view {.val {view_name}}.")

  invisible(NULL)
}
