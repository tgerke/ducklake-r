#' Create a data check from a rule
#'
#' **Experimental.** Stores a rule as a check: a view, in a schema set aside
#' for checks, that returns the rows breaking the rule, with the rule's label
#' as its comment. State the rule the way you would say it, as the condition
#' every row should meet. `create_check()` keeps the rows where it is false,
#' so the code reads like the label and a rule such as "dose is not 0" is
#' written `dose != 0`, with no double negative. [run_checks()] then counts
#' the failing rows of every check in the schema.
#'
#' @param .data A lazy table (a dplyr pipeline built on
#'   [get_ducklake_table()]) holding the rows to check. Read the table by its
#'   schema-qualified name, `get_ducklake_table("main.cars")`, so the check
#'   binds whichever database is current.
#' @param check_name The rule's id, which becomes the view's name and the
#'   `check` column of [run_checks()].
#' @param rule The rule, as an expression on the columns of `.data` that is
#'   `TRUE` for a row in good standing, such as `cyl %in% c(4, 6, 8)`.
#' @param label One sentence stating the rule, stored as the view's comment.
#' @param listing <[`tidy-select`][dplyr::dplyr_tidy_select]> The columns
#'   the check returns for a failing row: what someone needs to find the row
#'   and fix it. All columns by default.
#' @param schema_name The schema that holds the checks. Defaults to
#'   `"checks"`, or to the schema in `check_name` when that is qualified
#'   (`"qc.cyl_known"`). It is created if it does not exist.
#' @param replace Replace an existing check of the same name (default TRUE).
#'
#' @details
#' A row where the rule evaluates to `NA` passes, as it does under a SQL
#' `CHECK` constraint: `NOT (rule)` is not true for it. When a missing value
#' should fail, say so in the rule (`!is.na(dose) & dose != 0`) or give it a
#' check of its own.
#'
#' The schema, the view, and its label are written in one transaction, so a
#' new or revised check is one snapshot. Inside [with_transaction()] they
#' join the open transaction, which is how a change of rules gets an author
#' and a commit message.
#'
#' A check is an ordinary view, and `create_check()` is a convenience for
#' the common case of one rule about each row. A rule that is easier to
#' state as its failure (models that appear twice, visits without a
#' subject) is a pipeline that returns those rows, stored with
#' [create_view()] in the same schema and labelled with
#' [set_table_comment()]. Both forms select the same rows, since
#' three-valued logic treats `NOT (dose != 0)` and `dose = 0` alike, so
#' choose the one that reads better.
#'
#' @returns Invisibly returns `NULL`.
#' @export
#'
#' @seealso [run_checks()] to run the checks, [create_view()] for a check
#'   written as its failure, and `vignette("data-checks")`.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("create_check_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("create_check_lake", lake_path = lake_dir)
#' create_table(
#'   data.frame(model = rownames(mtcars), mtcars, row.names = NULL),
#'   "cars"
#' )
#'
#' get_ducklake_table("main.cars") |>
#'   create_check(
#'     "cyl_known", cyl %in% c(4, 6, 8),
#'     label = "cyl is 4, 6, or 8",
#'     listing = c(model, cyl)
#'   )
#'
#' # A rule that forbids a value is stated as it is said
#' get_ducklake_table("main.cars") |>
#'   create_check("hp_not_zero", hp != 0, label = "hp is not 0")
#'
#' run_checks()
#'
#' detach_ducklake("create_check_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
create_check <- function(.data, check_name, rule, label,
                         listing = dplyr::everything(),
                         schema_name = NULL, replace = TRUE) {
  if (!inherits(.data, "tbl_lazy")) {
    cli::cli_abort(c(
      "{.arg .data} must be a lazy table, e.g. a pipeline built on {.fun get_ducklake_table}.",
      "i" = "A check is a view over the lake's tables, so it cannot be made from a data frame."
    ))
  }
  if (!is.character(label) || length(label) != 1 || is.na(label) || !nzchar(label)) {
    cli::cli_abort("{.arg label} must be one sentence stating the rule.")
  }
  ref <- resolve_table_ref(check_name, schema_name)
  schema <- if (is.null(ref$schema)) "checks" else ref$schema
  view_name <- paste(schema, ref$table, sep = ".")

  conn <- dbplyr::remote_con(.data)
  ducklake_name <- infer_ducklake_name(NULL, conn)
  failing <- dplyr::select(dplyr::filter(.data, !({{ rule }})), {{ listing }})

  # One snapshot for the schema, the view, and its label
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
  quietly({
    if (!schema_exists(schema, ducklake_name, conn)) {
      create_schema(schema, ducklake_name = ducklake_name)
    }
    create_view(failing, view_name, replace = replace)
    set_table_comment(view_name, label)
  })
  if (own_txn) {
    DBI::dbExecute(conn, "COMMIT;")
    committed <- TRUE
  }
  dl_inform("Created check {.val {ref$table}} in {.val {schema}}: {label}")

  invisible(NULL)
}

#' Run code with the package's confirmations turned off
#' @noRd
quietly <- function(expr) {
  old <- options(ducklake.verbose = FALSE)
  on.exit(options(old), add = TRUE)
  force(expr)
}

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
#' Write a check with [create_check()], which takes the rule as it is said
#' and keeps the rows that break it, or with [create_view()] from a pipeline
#' that returns the failing rows itself, labelled with
#' [set_table_comment()]. A row where the rule is `NA` passes, so a missing
#' value needs a rule of its own. Read the table by its schema-qualified
#' name, `get_ducklake_table("main.cars")`: the view then binds whichever
#' database is current, for this function's `ducklake_name` and for other
#' clients of the lake.
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
#' @seealso [create_check()] to write a check, [with_transaction()] to gate
#'   a load on the result, and `vignette("data-checks")`.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("checks_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("checks_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' get_ducklake_table("main.cars") |>
#'   create_check("cyl_known", cyl %in% c(4, 6, 8), label = "cyl is 4, 6, or 8")
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
