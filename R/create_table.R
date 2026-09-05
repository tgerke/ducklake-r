#' Create a DuckLake table
#'
#' @param data_source Raw data source. Can be:
#'   - A URL (http:// or https://)
#'   - A file path (e.g., "data.csv", "data.parquet")
#'   - An R data.frame or tibble
#'   - A lazy table (tbl_duckdb_connection or tbl_lazy)
#' @param table_name Name of the new table
#' @param labels When `TRUE` (the default) and the data has haven/labelled
#'   variable labels (`label` attributes on columns), store them in the
#'   lake as column comments -- in the same transaction as the table
#'   creation, so both land as one snapshot. Collecting the table later
#'   restores the labels (see [get_table_comments()]), and every other
#'   client of the lake can read them too. Set to `FALSE` to skip.
#'
#' @returns Invisibly, `NULL`. Called for its side effect of creating the
#'   table in the lake.
#' @family table operations
#' @export
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("create_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("create_lake", lake_path = lake_dir)
#'
#' # From data.frame
#' create_table(mtcars, "cars")
#'
#' # From a local file
#' csv_path <- tempfile(fileext = ".csv")
#' utils::write.csv(mtcars, csv_path, row.names = FALSE)
#' create_table(csv_path, "cars_from_csv")
#'
#' # From a lazy table (pipe-friendly)
#' get_ducklake_table("cars") |>
#'   dplyr::filter(cyl > 4) |>
#'   create_table("big_cars")
#'
#' # From a URL -- needs network access and the httpfs extension
#' \dontrun{
#' create_table("https://example.com/data.csv", "remote_table")
#' }
#'
#' unlink(csv_path)
#'
#' detach_ducklake("create_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
create_table <- function(data_source, table_name, labels = TRUE) {
  # Handle lazy tables (tbl_duckdb_connection, tbl_lazy)
  if (inherits(data_source, "tbl_lazy")) {
    # Materialize the lazy table to a data.frame
    data_source <- dplyr::collect(data_source)
  }

  # Handle data.frame or tibble
  if (is.data.frame(data_source)) {
    prepared <- prepare_data_frame(data_source, labels)
    data_source <- prepared$data
    column_labels <- prepared$labels

    # Register the data.frame as a temporary view in DuckDB; unregister on
    # exit so a failed CREATE doesn't leave the view behind on the shared
    # connection
    conn <- get_ducklake_connection()
    temp_view_name <- register_temp_view(data_source, table_name, conn)
    on.exit(
      duckdb::duckdb_unregister(get_ducklake_connection(), temp_view_name),
      add = TRUE
    )

    # When labels ride along, the CREATE and the COMMENT statements must
    # land as one snapshot: open a transaction of our own unless the
    # caller already has one
    own_txn <- length(column_labels) > 0 && !in_transaction(conn)
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

    # Create the table from the temporary view
    db_execute(sprintf(
      "CREATE TABLE %s AS SELECT * FROM %s;",
      quote_ident(table_name), quote_ident(temp_view_name)
    ))

    store_column_labels(table_name, column_labels, conn)

    if (own_txn) {
      DBI::dbExecute(conn, "COMMIT;")
      committed <- TRUE
    }
    if (length(column_labels) > 0) {
      cli::cli_inform(
        "Stored {length(column_labels)} column label{?s} as column comment{?s}."
      )
    }

    return(invisible(NULL))
  }
  
  # If data_source is a URL, ensure httpfs extension is installed and loaded
  if (is.character(data_source) && grepl("^https?://", data_source)) {
    load_or_install_extension("httpfs")
  }
  
  # Handle file paths and URLs
  if (is.character(data_source)) {
    db_execute(sprintf(
      "CREATE TABLE %s AS FROM %s;",
      quote_ident(table_name), quote_sql(data_source)
    ))
  } else {
    cli::cli_abort("{.arg data_source} must be a character string (file path or URL) or a data.frame.")
  }
  
  invisible(NULL)
}

#' Prepare a data frame for loading into DuckLake
#'
#' Converts factor columns to character (DuckLake has no ENUM type) and,
#' when `labels` is `TRUE`, collects haven/labelled `label` attributes to
#' store as column comments.
#'
#' @param data A data frame.
#' @param labels Whether to collect column labels.
#' @returns A list with `data` and a named character vector `labels`.
#' @noRd
prepare_data_frame <- function(data, labels = TRUE) {
  # DuckLake does not support ENUM columns, which is what factors become
  # in DuckDB -- store them as character instead
  factor_cols <- vapply(data, is.factor, logical(1))
  if (any(factor_cols)) {
    data[factor_cols] <- lapply(data[factor_cols], as.character)
    cli::cli_inform(
      "Converted factor column{?s} {.field {names(data)[factor_cols]}} to character (DuckLake does not support ENUM columns)."
    )
  }

  column_labels <- character(0)
  if (isTRUE(labels)) {
    found <- vapply(
      data,
      function(col) {
        lbl <- attr(col, "label", exact = TRUE)
        if (is.character(lbl) && length(lbl) == 1 && !is.na(lbl)) {
          lbl
        } else {
          NA_character_
        }
      },
      character(1)
    )
    column_labels <- found[!is.na(found)]
  }

  list(data = data, labels = column_labels)
}

#' Register a data frame as a temporary DuckDB view
#'
#' The caller unregisters it (typically with `on.exit()`) so a failed
#' statement never leaves the view behind on the shared connection.
#'
#' @param data A data frame.
#' @param table_name The target table, used to derive the view name.
#' @param conn A DBI connection.
#' @returns The view name.
#' @noRd
register_temp_view <- function(data, table_name, conn) {
  temp_view_name <- paste0(
    "__temp_view_", gsub("[^a-zA-Z0-9]", "_", table_name)
  )
  duckdb::duckdb_register(conn, temp_view_name, data)
  temp_view_name
}

#' Store column labels as comments, in the open transaction
#' @noRd
store_column_labels <- function(table_name, column_labels, conn) {
  for (col in names(column_labels)) {
    db_execute(
      sprintf(
        "COMMENT ON COLUMN %s.%s IS %s;",
        quote_ident(table_name, conn),
        quote_column(col, conn),
        quote_sql(column_labels[[col]])
      ),
      conn = conn
    )
  }
  invisible(NULL)
}
