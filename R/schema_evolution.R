#' Add a column to a DuckLake table
#'
#' Adds a column in place with `ALTER TABLE ... ADD COLUMN`. This is a
#' metadata-only change: no data files are rewritten, history is preserved,
#' and earlier snapshots still show the old schema. Compare
#' [replace_table()], which collects the table into R and rewrites it.
#'
#' @param table_name The table to change.
#' @param column_name Name of the new column.
#' @param type SQL type for the new column, e.g. `"INTEGER"`,
#'   `"DECIMAL(10,2)"`, or `"TIMESTAMP WITH TIME ZONE"`.
#' @param default Optional default value (an R scalar: character, numeric,
#'   logical, Date, or POSIXct). In DuckLake the default applies to
#'   existing rows as well as future inserts, so the new column appears
#'   filled everywhere. Without a default, the column reads `NA` for
#'   existing rows.
#'
#' @returns Invisibly returns `NULL`.
#' @family schema evolution
#' @export
#'
#' @seealso [drop_table_column()], [rename_table_column()],
#'   [set_column_type()], [rename_ducklake_table()]
#'
#' @examples
#' \dontrun{
#' add_table_column("adsl", "AGECAT", "VARCHAR")
#' add_table_column("sales", "discount", "DECIMAL(5,2)", default = 0)
#' }
add_table_column <- function(table_name, column_name, type, default = NULL) {
  conn <- get_ducklake_connection()
  check_column_type(type)

  default_sql <- if (is.null(default)) {
    ""
  } else {
    sprintf(" DEFAULT %s", render_sql_literal(default))
  }
  db_execute(
    sprintf(
      "ALTER TABLE %s ADD COLUMN %s %s%s;",
      quote_ident(table_name, conn),
      quote_column(column_name, conn),
      type,
      default_sql
    ),
    conn = conn
  )
  cli::cli_inform(c(
    "Added column {.val {column_name}} ({type}) to {.val {table_name}}.",
    "i" = "Metadata-only change; no data files were rewritten."
  ))

  invisible(NULL)
}

#' Drop a column from a DuckLake table
#'
#' Removes a column in place with `ALTER TABLE ... DROP COLUMN`. This is a
#' metadata-only change: the column disappears from the current schema, but
#' earlier snapshots still contain it and remain queryable through time
#' travel.
#'
#' @param table_name The table to change.
#' @param column_name Name of the column to drop.
#'
#' @returns Invisibly returns `NULL`.
#' @family schema evolution
#' @export
#'
#' @seealso [add_table_column()], [rename_table_column()],
#'   [get_ducklake_table_version()] to read snapshots that still have the
#'   column
#'
#' @examples
#' \dontrun{
#' drop_table_column("adsl", "SCRATCH_FLAG")
#' }
drop_table_column <- function(table_name, column_name) {
  conn <- get_ducklake_connection()

  db_execute(
    sprintf(
      "ALTER TABLE %s DROP COLUMN %s;",
      quote_ident(table_name, conn),
      quote_column(column_name, conn)
    ),
    conn = conn
  )
  cli::cli_inform(
    "Dropped column {.val {column_name}} from {.val {table_name}}. Earlier snapshots still contain it."
  )

  invisible(NULL)
}

#' Rename a column in a DuckLake table
#'
#' Renames a column in place with `ALTER TABLE ... RENAME COLUMN`, a
#' metadata-only change.
#'
#' @param table_name The table to change.
#' @param from Current column name.
#' @param to New column name.
#'
#' @returns Invisibly returns `NULL`.
#' @family schema evolution
#' @export
#'
#' @seealso [add_table_column()], [drop_table_column()],
#'   [rename_ducklake_table()]
#'
#' @examples
#' \dontrun{
#' rename_table_column("adsl", from = "AGEGRP", to = "AGEGR1")
#' }
rename_table_column <- function(table_name, from, to) {
  conn <- get_ducklake_connection()

  db_execute(
    sprintf(
      "ALTER TABLE %s RENAME COLUMN %s TO %s;",
      quote_ident(table_name, conn),
      quote_column(from, conn, arg = "from"),
      quote_column(to, conn, arg = "to")
    ),
    conn = conn
  )
  cli::cli_inform(
    "Renamed column {.val {from}} to {.val {to}} in {.val {table_name}}."
  )

  invisible(NULL)
}

#' Rename a DuckLake table
#'
#' Renames a table in place with `ALTER TABLE ... RENAME TO`, a
#' metadata-only change.
#'
#' @param from Current table name.
#' @param to New table name.
#'
#' @details
#' History survives the rename, but snapshots from before it stay
#' associated with the old name: `get_ducklake_table_version()` on the new
#' name reaches back only as far as the rename, while the old name still
#' serves the earlier snapshots.
#'
#' @returns Invisibly returns `NULL`.
#' @family schema evolution
#' @export
#'
#' @seealso [rename_table_column()]
#'
#' @examples
#' \dontrun{
#' rename_ducklake_table("sales", "sales_daily")
#' }
rename_ducklake_table <- function(from, to) {
  conn <- get_ducklake_connection()

  db_execute(
    sprintf(
      "ALTER TABLE %s RENAME TO %s;",
      quote_ident(from, conn),
      quote_column(to, conn, arg = "to")
    ),
    conn = conn
  )
  cli::cli_inform(c(
    "Renamed table {.val {from}} to {.val {to}}.",
    "i" = "Snapshots from before the rename remain queryable under the old name."
  ))

  invisible(NULL)
}

#' Change the type of a DuckLake table column
#'
#' Changes a column's type in place with `ALTER TABLE ... ALTER COLUMN ...
#' SET TYPE`. DuckLake permits widening promotions only (for example
#' `INTEGER` to `BIGINT`, or `FLOAT` to `DOUBLE`): every existing value
#' must be representable in the new type, so no data can be lost and no
#' data files need rewriting.
#'
#' @param table_name The table to change.
#' @param column_name Name of the column.
#' @param type The new (wider) SQL type.
#'
#' @details
#' To *narrow* a type, which DuckLake refuses, take the explicit route:
#' [add_table_column()] with the smaller type, copy the values over (after
#' checking they fit), [drop_table_column()] the original, and
#' [rename_table_column()] the new column into place.
#'
#' @returns Invisibly returns `NULL`.
#' @family schema evolution
#' @export
#'
#' @seealso [add_table_column()], [drop_table_column()],
#'   [rename_table_column()]
#'
#' @examples
#' \dontrun{
#' set_column_type("sales", "order_id", "BIGINT")
#' }
set_column_type <- function(table_name, column_name, type) {
  conn <- get_ducklake_connection()
  check_column_type(type)

  sql <- sprintf(
    "ALTER TABLE %s ALTER COLUMN %s SET TYPE %s;",
    quote_ident(table_name, conn),
    quote_column(column_name, conn),
    type
  )
  tryCatch(
    db_execute(sql, conn = conn),
    error = function(e) {
      if (grepl("widening type promotions", conditionMessage(e))) {
        cli::cli_abort(
          c(
            "DuckLake only allows widening type changes, and {.val {type}} would narrow column {.val {column_name}}.",
            "i" = "To narrow: add a new column with the smaller type, copy the values over, drop the original, and rename the new column into place.",
            "i" = "See {.fun add_table_column}, {.fun drop_table_column}, and {.fun rename_table_column}."
          ),
          parent = e
        )
      }
      stop(e)
    }
  )
  cli::cli_inform(
    "Column {.val {column_name}} in {.val {table_name}} is now {type}."
  )

  invisible(NULL)
}

#' Validate a SQL type expression for schema evolution
#'
#' Admits plain type names, one-or-two-argument parameterized types such as
#' DECIMAL(10,2), multi-word types such as TIMESTAMP WITH TIME ZONE, and
#' array suffixes. Blocks anything that could smuggle in extra SQL; DuckDB
#' performs the real type validation.
#'
#' @param type The type string.
#' @param arg Argument name for error messages.
#' @returns `type`, invisibly.
#' @noRd
check_column_type <- function(type, arg = "type") {
  if (!is.character(type) || length(type) != 1 || is.na(type) ||
      !nzchar(type)) {
    cli::cli_abort("{.arg {arg}} must be a single SQL type name.")
  }
  ok <- grepl(
    "^[A-Za-z][A-Za-z0-9_ ]*(\\([0-9]+(\\s*,\\s*[0-9]+)?\\))?(\\[\\])*$",
    type
  )
  if (!ok) {
    cli::cli_abort(c(
      "{.arg {arg}} must be a plain SQL type such as {.val INTEGER}, {.val DECIMAL(10,2)}, or {.val TIMESTAMP WITH TIME ZONE}.",
      "x" = "Got {.val {type}}.",
      "i" = "For types this guard cannot express, run the ALTER TABLE statement directly with {.fun DBI::dbExecute}."
    ))
  }
  invisible(type)
}
