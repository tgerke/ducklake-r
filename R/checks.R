#' Run the data checks stored in a schema
#'
#' **Experimental.** Counts the rows returned by each view in a schema set
#' aside for data checks. The convention: a check is a view that returns the
#' rows breaking a rule, the view's name is the rule's id, and its comment is
#' the rule's label. Checks written this way live in the DuckLake catalog,
#' so they are versioned with the data and every client of the lake can run
#' them. The interface may change while the convention settles.
#'
#' @param schema_name The schema that holds the check views (default
#'   `"checks"`).
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @details
#' Write a check with [create_view()] from a pipeline that keeps the failing
#' rows, such as `filter(!(cyl %in% c(4, 6, 8)))`, and label it with
#' [set_table_comment()]. `NOT (condition)` is not true for a row where the
#' condition is `NA`, so a missing value passes unless a check of its own
#' looks for it. Read the table by its schema-qualified name,
#' `get_ducklake_table("main.cars")`: the view then binds whichever database
#' is current, for this function's `ducklake_name` and for other clients of
#' the lake.
#'
#' Every view in the schema counts as a check, so keep other views
#' elsewhere. A view that summarizes, returning a row of totals, reports a
#' failure every time. A check whose view no longer binds, after a column
#' rename for instance, is an error and not a pass; DuckDB's message quotes
#' the line naming the view.
#'
#' Inside [with_transaction()] the counts include the transaction's pending
#' writes, so a load can be checked before it commits: `stop()` when a check
#' fails and the transaction rolls back. On a lake attached with
#' `snapshot_version` or `snapshot_time`, the checks and the data are both
#' read as of that snapshot.
#'
#' @returns A data frame with one row per check, ordered by name: `check`
#'   (the view name), `label` (the view's comment, `NA` without one), and
#'   `n_fail` (the number of rows the view returns). Zero rows when the
#'   schema holds no views.
#' @export
#'
#' @seealso [create_view()] and [set_table_comment()] to write a check,
#'   [with_transaction()] to gate a load on the result, and
#'   `vignette("data-checks")`.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("checks_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("checks_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' # A check is a view of the rows that break a rule
#' create_schema("checks")
#' get_ducklake_table("main.cars") |>
#'   dplyr::filter(!(cyl %in% c(4, 6, 8))) |>
#'   create_view("checks.cyl_known")
#' set_table_comment("checks.cyl_known", "cyl is 4, 6, or 8")
#'
#' run_checks()
#'
#' detach_ducklake("checks_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
run_checks <- function(schema_name = "checks", ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)
  schema <- quote_column(schema_name, conn, arg = "schema_name")

  # A mistyped schema must not pass a gate as "no checks failed"
  if (!schema_exists(schema_name, ducklake_name, conn)) {
    cli::cli_abort(c(
      "Schema {.val {schema_name}} does not exist in {.val {ducklake_name}}.",
      "i" = "Checks live in a schema of their own; create it with {.fun create_schema}."
    ))
  }

  # duckdb_views(), not the metadata tables: it shows a view created in the
  # open transaction and follows a snapshot-pinned attach
  views <- DBI::dbGetQuery(
    conn,
    "SELECT view_name, comment FROM duckdb_views()
     WHERE database_name = ? AND schema_name = ? AND NOT internal
     ORDER BY view_name",
    params = list(ducklake_name, schema_name)
  )
  if (nrow(views) == 0) {
    return(data.frame(
      check = character(), label = character(), n_fail = numeric()
    ))
  }

  # One branch per line, so a binder error quotes the view that broke
  branches <- sprintf(
    "SELECT %d AS i, count(*) AS n_fail FROM %s.%s.%s",
    seq_len(nrow(views)),
    quote_ident(ducklake_name, conn),
    schema,
    vapply(views$view_name, quote_column, character(1), conn = conn,
           USE.NAMES = FALSE)
  )
  counts <- DBI::dbGetQuery(
    conn,
    paste0(paste(branches, collapse = "\nUNION ALL\n"), "\nORDER BY i")
  )

  data.frame(
    check = views$view_name,
    label = views$comment,
    n_fail = counts$n_fail
  )
}
