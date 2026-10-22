#' Create a schema in a DuckLake
#'
#' Schemas group the tables of a lake, and are the natural home for
#' medallion layers (`bronze`, `silver`, `gold`) or per-study areas. Every
#' function that takes a table name accepts `"schema.table"`, and
#' [list_ducklake_tables()] shows each table's schema. DuckLake writes a
#' schema's data files under a directory of their own, which is what
#' path-based access control on object storage keys on.
#'
#' Schemas nest (DuckLake 1.1): `"sales.europe"` is the schema `europe`
#' inside `sales`, and its tables are named `"sales.europe.orders"`. Every
#' dot in a name is a level, so the parents are created first when they
#' are missing, and a table, schema, or `schema_name` argument anywhere in
#' the package takes the same dotted path. A first component that names an
#' attached lake is read as the lake (`"lake.sales.orders"`). One limit is
#' DuckDB's rather than DuckLake's: `COMMENT ON` takes at most three
#' qualifiers, so comments and variable labels reach a table one level deep
#' and no deeper; [create_table()] warns and drops the labels of a deeper
#' table. A lake on catalog format 1.0 has no nested schemas; see
#' `attach_ducklake(automatic_migration = )`.
#'
#' @param schema_name Name of the schema, or a dotted path for a nested
#'   one.
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
  components <- schema_components(schema_name)

  if (schema_exists(schema_name, ducklake_name, conn)) {
    if (!if_not_exists) {
      cli::cli_abort("Schema {.val {schema_name}} already exists in {.val {ducklake_name}}.")
    }
    dl_inform("Schema {.val {schema_name}} already exists.")
    return(invisible(NULL))
  }

  # DuckDB refuses CREATE SCHEMA IF NOT EXISTS on a nested schema whose
  # parent is missing, so the parents are created first, outermost in
  created <- character()
  for (depth in seq_along(components)) {
    path <- paste(components[seq_len(depth)], collapse = ".")
    if (depth < length(components) && schema_exists(path, ducklake_name, conn)) {
      next
    }
    tryCatch(
      db_execute(
        sprintf(
          "CREATE SCHEMA %s;",
          quote_ident(paste(ducklake_name, path, sep = "."), conn)
        ),
        conn = conn
      ),
      error = function(e) {
        if (grepl("does not support nested schemas", conditionMessage(e), fixed = TRUE)) {
          cli::cli_abort(c(
            "Lake {.val {ducklake_name}} uses DuckLake format 1.0, which has no nested schemas.",
            "i" = "Attach it with {.code automatic_migration = TRUE} to move it to format 1.1 (a permanent step; back it up first), or use a top-level schema name."
          ), parent = e)
        }
        stop(e)
      }
    )
    created <- c(created, path)
  }
  if (length(created) > 1) {
    dl_inform("Created schema {.val {schema_name}} and its parent{?s} {.val {created[-length(created)]}}.")
  } else {
    dl_inform("Created schema {.val {schema_name}}.")
  }

  invisible(NULL)
}

#' The components of a dotted schema path, validated
#' @noRd
schema_components <- function(schema_name, arg = "schema_name") {
  if (!is.character(schema_name) || length(schema_name) != 1 ||
      is.na(schema_name) || !nzchar(schema_name)) {
    cli::cli_abort("{.arg {arg}} must be a single, non-empty string.")
  }
  components <- strsplit(schema_name, ".", fixed = TRUE)[[1]]
  if (length(components) == 0 || any(!nzchar(components))) {
    cli::cli_abort(
      "{.arg {arg}} has an empty name component: {.val {schema_name}}."
    )
  }
  components
}

#' Drop a schema from a DuckLake
#'
#' Removes a schema with `DROP SCHEMA`. By default the schema must be
#' empty; `cascade = TRUE` drops its tables and views, and the schemas
#' nested inside it, with it. Like every change, the drop is a snapshot,
#' so the tables stay reachable through time travel.
#'
#' @param schema_name Name of the schema, or the dotted path of a nested
#'   one.
#' @param cascade Also drop the tables, views, and nested schemas in the
#'   schema (default `FALSE`).
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
  schema_components(schema_name)

  db_execute(
    sprintf(
      "DROP SCHEMA %s%s;",
      quote_ident(paste(ducklake_name, schema_name, sep = "."), conn),
      if (isTRUE(cascade)) " CASCADE" else ""
    ),
    conn = conn
  )
  dl_inform("Dropped schema {.val {schema_name}}.")

  invisible(NULL)
}

#' Does a schema path exist in an attached lake?
#' @noRd
schema_exists <- function(schema_name, ducklake_name, conn) {
  found <- DBI::dbGetQuery(
    conn,
    sprintf(
      "WITH RECURSIVE %s SELECT 1 FROM schema_paths WHERE schema_path = ?",
      catalog_schema_paths_cte(ducklake_name)
    ),
    params = list(schema_name)
  )
  nrow(found) > 0
}
