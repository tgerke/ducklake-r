#' Describe an attached DuckLake
#'
#' One row of facts about a lake: which catalog backend it uses and where
#' the catalog and data live, the DuckLake format version the catalog was
#' written in, the extension version reading it, whether its Parquet files
#' are encrypted, and the current snapshot id. Handy at the top of a report
#' or a pipeline log, and the first thing to look at when a lake behaves
#' unexpectedly.
#'
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @returns A one-row data frame with `ducklake_name`, `backend`,
#'   `catalog` (the catalog file or connection string), `data_path`,
#'   `format_version`, `extension_version`, `encrypted`, and
#'   `current_snapshot`.
#' @family connection management
#' @export
#'
#' @seealso [get_ducklake_options()] for the options set on the lake,
#'   [list_ducklake_tables()] for what it holds.
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("info_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("info_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' get_ducklake_info()
#'
#' detach_ducklake("info_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
get_ducklake_info <- function(ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  settings <- DBI::dbGetQuery(
    conn,
    sprintf("SELECT * FROM ducklake_settings(%s)", quote_sql(ducklake_name))
  )
  snapshot <- DBI::dbGetQuery(
    conn,
    sprintf("FROM %s.current_snapshot()", quote_ident(ducklake_name, conn))
  )[[1]]

  options <- get_ducklake_options(ducklake_name)
  global <- options[options$scope == "GLOBAL", , drop = FALSE]
  value_of <- function(key) {
    value <- global$value[global$option_name == key]
    if (length(value) > 0) value[[1]] else NA_character_
  }

  backend <- get_ducklake_backend(ducklake_name)
  entry <- .ducklake_env$lakes[[ducklake_name]]
  catalog <- if (!is.null(entry$catalog_connection_string)) {
    entry$catalog_connection_string
  } else if (backend == "duckdb") {
    file.path(sub("/+$", "", settings$data_path), paste0(ducklake_name, ".ducklake"))
  } else {
    NA_character_
  }

  data.frame(
    ducklake_name = ducklake_name,
    backend = backend,
    catalog = catalog,
    data_path = settings$data_path,
    format_version = value_of("version"),
    extension_version = settings$extension_version,
    encrypted = identical(value_of("encrypted"), "true"),
    current_snapshot = as.integer(snapshot),
    stringsAsFactors = FALSE
  )
}
