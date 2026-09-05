#' Create a schema in a DuckLake
#'
#' Schemas group the tables of a lake, and are the natural home for
#' medallion layers (`bronze`, `silver`, `gold`) or per-study areas. Every
#' function that takes a table name accepts `"schema.table"`, and
#' [list_ducklake_tables()] shows each table's schema. DuckLake writes a
#' schema's data files under a directory of their own, which is what
#' path-based access control on object storage keys on.
#'
#' @param schema_name Name of the schema.
#' @param if_not_exists When the schema already exists, say so and do
#'   nothing (default `TRUE`) rather than error.
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @returns Invisibly, `NULL`.
#' @family table operations
#' @export
#'
#' @seealso [drop_schema()], [list_ducklake_tables()]
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("schema_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("schema_lake", lake_path = lake_dir)
#'
#' # One schema per medallion layer
#' create_schema("bronze")
#' create_schema("silver")
#'
#' create_table(mtcars, "bronze.cars")
#' get_ducklake_table("bronze.cars") |>
#'   dplyr::filter(cyl == 4) |>
#'   create_table("silver.cars")
#'
#' list_ducklake_tables()
#'
#' detach_ducklake("schema_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
create_schema <- function(schema_name, if_not_exists = TRUE, ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)
  quoted <- quote_column(schema_name, conn, arg = "schema_name")

  if (schema_exists(schema_name, ducklake_name, conn)) {
    if (!if_not_exists) {
      cli::cli_abort("Schema {.val {schema_name}} already exists in {.val {ducklake_name}}.")
    }
    cli::cli_inform("Schema {.val {schema_name}} already exists.")
    return(invisible(NULL))
  }

  db_execute(
    sprintf("CREATE SCHEMA %s.%s;", quote_ident(ducklake_name, conn), quoted),
    conn = conn
  )
  cli::cli_inform("Created schema {.val {schema_name}}.")

  invisible(NULL)
}

#' Drop a schema from a DuckLake
#'
#' Removes a schema with `DROP SCHEMA`. By default the schema must be
#' empty; `cascade = TRUE` drops its tables and views with it. Like every
#' change, the drop is a snapshot, so the tables stay reachable through
#' time travel.
#'
#' @param schema_name Name of the schema.
#' @param cascade Also drop the tables and views in the schema (default
#'   `FALSE`).
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @returns Invisibly, `NULL`.
#' @family table operations
#' @export
#'
#' @seealso [create_schema()]
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("dropschema_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("dropschema_lake", lake_path = lake_dir)
#'
#' create_schema("scratch")
#' create_table(mtcars, "scratch.cars")
#' drop_schema("scratch", cascade = TRUE)
#'
#' detach_ducklake("dropschema_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
drop_schema <- function(schema_name, cascade = FALSE, ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  db_execute(
    sprintf(
      "DROP SCHEMA %s.%s%s;",
      quote_ident(ducklake_name, conn),
      quote_column(schema_name, conn, arg = "schema_name"),
      if (isTRUE(cascade)) " CASCADE" else ""
    ),
    conn = conn
  )
  cli::cli_inform("Dropped schema {.val {schema_name}}.")

  invisible(NULL)
}

#' Does a schema exist in an attached lake?
#' @noRd
schema_exists <- function(schema_name, ducklake_name, conn) {
  found <- DBI::dbGetQuery(
    conn,
    "SELECT 1 FROM duckdb_schemas() WHERE database_name = ? AND schema_name = ?",
    params = list(ducklake_name, schema_name)
  )
  nrow(found) > 0
}
