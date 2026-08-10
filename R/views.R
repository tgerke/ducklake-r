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
#'
#' @details
#' Read a view back with [get_ducklake_table()], which works for views and
#' tables alike, and keep piping dplyr verbs onto it. Like tables, views
#' are versioned: dropping or replacing one is a snapshot like any other.
#'
#' @returns Invisibly returns `NULL`.
#' @family table operations
#' @export
#'
#' @seealso [drop_view()], [list_ducklake_tables()],
#'   [replace_table()] to materialize a pipeline as data instead.
#'
#' @examples
#' \dontrun{
#' # Encapsulate filtering logic the whole team should share
#' get_ducklake_table("adsl") |>
#'   filter(SAFFL == "Y") |>
#'   select(USUBJID, TRT01A, AGE) |>
#'   create_view("v_safety_population")
#'
#' # Reads run the stored query against current data
#' get_ducklake_table("v_safety_population") |> collect()
#' }
create_view <- function(.data, view_name, replace = TRUE) {
  if (!inherits(.data, "tbl_lazy")) {
    cli::cli_abort(c(
      "{.arg .data} must be a lazy table, e.g. a pipeline built on {.fun get_ducklake_table}.",
      "i" = "A view stores a query, not data. To store data, use {.fun create_table}.",
      "i" = "To create a view from raw SQL, use {.fun DBI::dbExecute} directly."
    ))
  }

  conn <- dbplyr::remote_con(.data)
  db_execute(
    sprintf(
      "CREATE %sVIEW %s AS\n%s;",
      if (isTRUE(replace)) "OR REPLACE " else "",
      quote_ident(view_name, conn),
      dbplyr::sql_render(.data, conn)
    ),
    conn = conn
  )
  cli::cli_inform("Created view {.val {view_name}}.")

  invisible(NULL)
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
#' @examples
#' \dontrun{
#' drop_view("v_safety_population")
#' }
drop_view <- function(view_name) {
  conn <- get_ducklake_connection()

  db_execute(
    sprintf("DROP VIEW %s;", quote_ident(view_name, conn)),
    conn = conn
  )
  cli::cli_inform("Dropped view {.val {view_name}}.")

  invisible(NULL)
}
