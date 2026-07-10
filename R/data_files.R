#' Register existing Parquet files with a DuckLake table
#'
#' Adds Parquet files that already exist on disk (or object storage) to a
#' DuckLake table without copying or rewriting them. This is the migration
#' path for data that is already in Parquet: the files are recorded in the
#' catalog in place, and one snapshot is created per file added.
#'
#' @param table_name The table to add the files to. It must already exist
#'   with a schema compatible with the files (see `allow_missing` and
#'   `ignore_extra_columns` for the two permitted mismatches).
#' @param files Character vector of Parquet file paths or URIs.
#' @param schema_name Optional schema containing the table (defaults to the
#'   lake's `main` schema).
#' @param allow_missing If `TRUE`, files may lack columns that exist in the
#'   table; missing columns read as the column's initial default. Default
#'   `FALSE`.
#' @param ignore_extra_columns If `TRUE`, files may contain columns that the
#'   table does not have; the extra columns are inaccessible. Default
#'   `FALSE`.
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @details
#' Runs `CALL ducklake_add_data_files(...)` once per file. Ownership of each
#' file transfers to DuckLake: compaction (e.g.
#' [merge_adjacent_files()]) may later rewrite and delete it, so do not add
#' files that something else still relies on.
#'
#' @returns Invisibly returns the character vector of files added.
#' @family table operations
#' @export
#'
#' @seealso [list_ducklake_files()], [create_table()]
#'
#' @examples
#' \dontrun{
#' # Bring an existing Parquet extract into the lake without copying it
#' create_table(data.frame(id = integer(), value = numeric()), "readings")
#' add_data_files("readings", "extracts/readings_2026.parquet")
#'
#' # Several files at once, tolerating a column the table doesn't have
#' add_data_files(
#'   "readings",
#'   c("extracts/jan.parquet", "extracts/feb.parquet"),
#'   ignore_extra_columns = TRUE
#' )
#' }
add_data_files <- function(table_name,
                           files,
                           schema_name = NULL,
                           allow_missing = FALSE,
                           ignore_extra_columns = FALSE,
                           ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  if (!is.character(files) || length(files) == 0 || anyNA(files)) {
    cli::cli_abort("{.arg files} must be a character vector of file paths.")
  }

  extra <- paste0(
    if (!is.null(schema_name)) {
      sprintf(", schema => %s", quote_sql(schema_name))
    } else {
      ""
    },
    if (allow_missing) ", allow_missing => true" else "",
    if (ignore_extra_columns) ", ignore_extra_columns => true" else ""
  )

  for (file in files) {
    db_execute(
      sprintf(
        "CALL ducklake_add_data_files(%s, %s, %s%s);",
        quote_sql(ducklake_name),
        quote_sql(table_name),
        quote_sql(file),
        extra
      ),
      conn = conn
    )
  }

  cli::cli_inform(c(
    "Added {length(files)} file{?s} to table {.val {table_name}}.",
    "i" = "DuckLake now owns the added file{cli::qty(length(files))}{?s}; compaction may rewrite or delete {?it/them}."
  ))

  invisible(files)
}

#' List the data files backing a DuckLake table
#'
#' Returns the Parquet data files (and any delete files) that make up a
#' table, optionally as of a past snapshot.
#'
#' @param table_name The table whose files to list.
#' @param schema_name Optional schema containing the table (defaults to the
#'   lake's `main` schema).
#' @param snapshot_version Optional snapshot id: list the files as of that
#'   snapshot. Mutually exclusive with `snapshot_time`.
#' @param snapshot_time Optional POSIXct or UTC timestamp string: list the
#'   files as of that moment. Mutually exclusive with `snapshot_version`.
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @details
#' Wraps `ducklake_list_files()`. For per-table file counts and sizes across
#' the whole lake, see [get_table_info()]; for a picture of storage layout,
#' see [plot_table_files()].
#'
#' @returns A data frame with one row per data file, including `data_file`,
#'   `data_file_size_bytes`, and the associated `delete_file` columns
#'   (`NA` when a file has no deletes).
#' @family maintenance
#' @export
#'
#' @seealso [add_data_files()], [get_table_info()]
#'
#' @examples
#' \dontrun{
#' # Files behind a table right now
#' list_ducklake_files("readings")
#'
#' # Files as of an earlier snapshot
#' list_ducklake_files("readings", snapshot_version = 3)
#' }
list_ducklake_files <- function(table_name,
                                schema_name = NULL,
                                snapshot_version = NULL,
                                snapshot_time = NULL,
                                ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  if (!is.null(snapshot_version) && !is.null(snapshot_time)) {
    cli::cli_abort(
      "Provide only one of {.arg snapshot_version} and {.arg snapshot_time}."
    )
  }

  extra <- paste0(
    if (!is.null(schema_name)) {
      sprintf(", schema => %s", quote_sql(schema_name))
    } else {
      ""
    },
    if (!is.null(snapshot_version)) {
      sprintf(", snapshot_version => %d", as.integer(snapshot_version))
    } else {
      ""
    },
    if (!is.null(snapshot_time)) {
      sprintf(
        ", snapshot_time => TIMESTAMP %s",
        quote_sql(format_timestamp(snapshot_time))
      )
    } else {
      ""
    }
  )

  DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT * FROM ducklake_list_files(%s, %s%s);",
      quote_sql(ducklake_name),
      quote_sql(table_name),
      extra
    )
  )
}
