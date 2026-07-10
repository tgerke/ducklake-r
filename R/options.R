#' Set a DuckLake option
#'
#' Sets a DuckLake configuration option, either lake-wide or scoped to a
#' schema or table. Options are persisted in the metadata catalog, so they
#' survive detach/attach cycles and apply to every client of the lake.
#'
#' @param option Name of the option, e.g. `"parquet_compression"`,
#'   `"target_file_size"`, `"sort_on_insert"`, or
#'   `"data_inlining_row_limit"`. See
#'   \url{https://ducklake.select/docs/stable/duckdb/usage/configuration}
#'   for the full list.
#' @param value The value to set. Logicals are rendered as `true`/`false`,
#'   numbers as numeric literals, and everything else as a quoted string.
#' @param table_name Optional table name to scope the option to one table.
#' @param schema_name Optional schema name to scope the option to one schema
#'   (or, together with `table_name`, to qualify the table).
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @details
#' Table-scoped settings override schema-scoped ones, which override the
#' lake-wide default. Runs `CALL <lake>.set_option(...)`.
#'
#' Commonly tuned options include `parquet_compression` (default
#' `"snappy"`; `"zstd"` trades write speed for smaller files),
#' `target_file_size` (default `"512MB"`), `sort_on_insert` (default
#' `TRUE`; see [set_table_sorting()]), and `require_commit_message`
#' (default `FALSE`).
#'
#' @returns Invisibly returns `NULL`.
#' @family options
#' @export
#'
#' @seealso [get_ducklake_options()], [set_inlining_row_limit()]
#'
#' @examples
#' \dontrun{
#' # Smaller files at some write cost, lake-wide
#' set_ducklake_option("parquet_compression", "zstd")
#'
#' # Make every snapshot carry a commit message
#' set_ducklake_option("require_commit_message", TRUE)
#'
#' # Skip one table during compaction
#' set_ducklake_option("auto_compact", FALSE, table_name = "audit_log")
#' }
set_ducklake_option <- function(option,
                                value,
                                table_name = NULL,
                                schema_name = NULL,
                                ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  if (!is.character(option) || length(option) != 1 ||
      !grepl("^[a-z][a-z0-9_]*$", option)) {
    cli::cli_abort(
      "{.arg option} must be a single option name in snake_case."
    )
  }

  call_sql <- sprintf(
    "CALL %s.set_option(%s, %s%s);",
    ducklake_name,
    quote_sql(option),
    render_option_value(value),
    option_scope_args(table_name, schema_name)
  )
  db_execute(call_sql, conn = conn)

  scope <- if (!is.null(table_name)) {
    "table {.val {table_name}}"
  } else if (!is.null(schema_name)) {
    "schema {.val {schema_name}}"
  } else {
    "lake {.val {ducklake_name}}"
  }
  cli::cli_inform(paste0(
    "Option {.val {option}} set to {.val {value}} for ", scope, "."
  ))

  invisible(NULL)
}

#' List the options set on a DuckLake
#'
#' Reads the configuration options recorded in the metadata catalog,
#' including their scope (global, schema, or table).
#'
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @returns A data frame with one row per option setting, including
#'   `option_name`, `value`, `scope` (`GLOBAL`, `SCHEMA`, or `TABLE`), and
#'   `scope_entry`. Options left at their defaults are not listed.
#' @family options
#' @export
#'
#' @seealso [set_ducklake_option()]
#'
#' @examples
#' \dontrun{
#' get_ducklake_options()
#' }
get_ducklake_options <- function(ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  DBI::dbGetQuery(
    conn,
    sprintf("SELECT * FROM ducklake_options(%s);", quote_sql(ducklake_name))
  )
}

#' Render an R value as a set_option() SQL literal
#'
#' @noRd
render_option_value <- function(value) {
  if (length(value) != 1 || is.na(value)) {
    cli::cli_abort("{.arg value} must be a single non-missing value.")
  }
  if (is.logical(value)) {
    if (value) "true" else "false"
  } else if (is.numeric(value)) {
    format(value, scientific = FALSE)
  } else {
    quote_sql(as.character(value))
  }
}

#' Render optional schema/table scoping for set_option()
#'
#' @noRd
option_scope_args <- function(table_name, schema_name) {
  paste0(
    if (!is.null(schema_name)) {
      sprintf(", schema => %s", quote_sql(schema_name))
    } else {
      ""
    },
    if (!is.null(table_name)) {
      sprintf(", table_name => %s", quote_sql(table_name))
    } else {
      ""
    }
  )
}
