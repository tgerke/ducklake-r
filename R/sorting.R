#' Set the sort order of a table
#'
#' Declares how a table's data files should be sorted. DuckLake sorts data
#' on insert (unless the `sort_on_insert` option is disabled), during
#' compaction with [merge_adjacent_files()], and when flushing inlined data
#' with [flush_inlined_data()]. Sorted files carry tighter min/max
#' statistics, so filters on the sort columns prune files instead of
#' scanning them -- the complement to [set_table_partitioning()] for
#' high-cardinality columns.
#'
#' @param table_name The name of the table to sort.
#' @param sort_by Character vector of sort keys. Each entry is a column
#'   name, optionally followed by `ASC` or `DESC` and by `NULLS FIRST` or
#'   `NULLS LAST`, e.g. `"event_time DESC"` or `"id ASC NULLS LAST"`.
#'
#' @details
#' Runs `ALTER TABLE ... SET SORTED BY (...)`. Only newly written files are
#' sorted; existing files keep their layout until compaction rewrites them.
#'
#' DuckLake also accepts arbitrary SQL expressions as sort keys; this
#' wrapper deliberately accepts only column-based keys so the input can be
#' validated. For expression keys, run the `ALTER TABLE` statement directly
#' with [DBI::dbExecute()].
#'
#' To keep insert speed and sort the files only at compaction time, disable
#' sorting on insert with
#' `set_ducklake_option("sort_on_insert", FALSE, table_name = ...)`.
#'
#' @returns Invisibly returns `NULL`.
#' @family sorting
#' @export
#'
#' @seealso [reset_table_sorting()], [set_ducklake_option()],
#'   [set_table_partitioning()]
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("sort_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("sort_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' # Order rows so range filters can prune files
#' set_table_sorting("cars", "mpg")
#'
#' # Compound key with explicit directions
#' set_table_sorting("cars", c("cyl ASC", "mpg DESC"))
#'
#' detach_ducklake("sort_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
set_table_sorting <- function(table_name, sort_by) {
  conn <- get_ducklake_connection()

  if (!is.character(sort_by) || length(sort_by) == 0 || anyNA(sort_by)) {
    cli::cli_abort(
      "{.arg sort_by} must be a character vector of sort expressions."
    )
  }

  ident <- "[A-Za-z_][A-Za-z0-9_]*"
  allowed <- sprintf(
    "^%s(\\s+(ASC|DESC))?(\\s+NULLS\\s+(FIRST|LAST))?$",
    ident
  )
  ok <- grepl(allowed, sort_by, ignore.case = TRUE)
  if (!all(ok)) {
    cli::cli_abort(c(
      "Invalid sort expression{?s}: {.val {sort_by[!ok]}}.",
      "i" = "Supported form: a column name, optionally followed by {.code ASC}/{.code DESC} and {.code NULLS FIRST}/{.code NULLS LAST}."
    ))
  }

  db_execute(
    sprintf(
      "ALTER TABLE %s SET SORTED BY (%s);",
      quote_ident(table_name, conn),
      paste(sort_by, collapse = ", ")
    ),
    conn = conn
  )
  cli::cli_inform(c(
    "Table {.val {table_name}} is now sorted by {.val {sort_by}}.",
    "i" = "Only newly written data is sorted; existing files keep their layout until compaction."
  ))

  invisible(NULL)
}

#' Remove the sort order from a table
#'
#' Clears a table's declared sort order so newly written data files are no
#' longer sorted. Existing files are unaffected.
#'
#' @param table_name The name of the table.
#'
#' @returns Invisibly returns `NULL`.
#' @family sorting
#' @export
#'
#' @seealso [set_table_sorting()]
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("unsort_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("unsort_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' set_table_sorting("cars", "mpg")
#' reset_table_sorting("cars")
#'
#' detach_ducklake("unsort_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
reset_table_sorting <- function(table_name) {
  conn <- get_ducklake_connection()

  db_execute(
    sprintf(
      "ALTER TABLE %s RESET SORTED BY;",
      quote_ident(table_name, conn)
    ),
    conn = conn
  )
  cli::cli_inform("Sort order removed from table {.val {table_name}}.")

  invisible(NULL)
}
