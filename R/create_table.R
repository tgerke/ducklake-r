#' Create a DuckLake table
#'
#' @param data_source Raw data source. Can be:
#'   - A URL (http:// or https://)
#'   - A file path (e.g., "data.csv", "data.parquet")
#'   - An R data.frame or tibble
#'   - A lazy table (tbl_duckdb_connection or tbl_lazy). A lazy table on
#'     the package's connection, such as a dplyr pipeline built on
#'     [get_ducklake_table()], is written with `CREATE TABLE ... AS` inside
#'     DuckDB, so its rows never pass through R. A lazy table on another
#'     connection is collected first.
#' @param table_name Name of the new table
#' @param labels When `TRUE` (the default), store variable labels in the
#'   lake as column comments, in the same transaction as the table
#'   creation, so both land as one snapshot. For a data frame the labels
#'   are its haven/labelled `label` attributes; for a lazy table, each
#'   output column keeps the comment of the same-named column in the tables
#'   the query reads, so labels follow the data through a pipeline (a
#'   renamed or derived column starts without one). Collecting the table
#'   later restores the labels (see [get_table_comments()]), and every
#'   other client of the lake can read them too. Set to `FALSE` to skip.
#' @param partition_by Optional character vector of partition expressions,
#'   as for [set_table_partitioning()]: column names, `year(col)` and the
#'   other calendar transforms, or `bucket(n, col)`. The keys go on before
#'   the rows, so the first load is already partitioned, where
#'   [set_table_partitioning()] on an existing table affects only later
#'   writes.
#' @param sort_by Optional character vector of sort expressions, as for
#'   [set_table_sorting()]: a column name, optionally followed by `ASC` or
#'   `DESC` and `NULLS FIRST` or `NULLS LAST`. The first load is written in
#'   this order.
#' @param options Optional named list of table-scoped options, as for
#'   [set_ducklake_option()], for example `list(parquet_compression =
#'   "zstd", data_inlining_row_limit = 0)`. They are set before the rows
#'   are written and so govern the first load too: an inlining limit of
#'   `0` here sends even a handful of rows to a Parquet file instead of the
#'   catalog.
#'
#' @details
#' With any of `partition_by`, `sort_by`, or `options`, the table is
#' created empty from the source's columns, the keys and options go on,
#' and the rows are inserted, in one transaction (yours when one is open),
#' so the result is one snapshot with the layout in force from the first
#' row. Without them, a single `CREATE TABLE ... AS` does the work.
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
#' # From a lazy table: the query runs inside DuckDB and writes straight
#' # into the lake, without collecting into R
#' get_ducklake_table("cars") |>
#'   dplyr::filter(cyl > 4) |>
#'   create_table("big_cars")
#'
#' # Partitioned, sorted, and compressed from the first load
#' create_table(
#'   mtcars, "cars_by_cyl",
#'   partition_by = "cyl", sort_by = "mpg DESC",
#'   options = list(parquet_compression = "zstd")
#' )
#' get_table_partitions("cars_by_cyl")
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
create_table <- function(data_source, table_name, labels = TRUE,
                         partition_by = NULL, sort_by = NULL,
                         options = NULL) {
  layout <- check_table_layout(partition_by, sort_by, options)
  conn <- get_ducklake_connection()

  if (inherits(data_source, "tbl_lazy")) {
    if (same_connection(data_source, conn)) {
      return(create_table_from_query(data_source, table_name, labels, layout, conn))
    }
    # A lazy table on another connection has to come through R
    data_source <- dplyr::collect(data_source)
  }

  # Handle data.frame or tibble
  if (is.data.frame(data_source)) {
    prepared <- prepare_data_frame(data_source, labels)

    # Register the data.frame as a temporary view in DuckDB; unregister on
    # exit so a failed CREATE doesn't leave the view behind on the shared
    # connection
    temp_view_name <- register_temp_view(prepared$data, table_name, conn)
    on.exit(
      duckdb::duckdb_unregister(get_ducklake_connection(), temp_view_name),
      add = TRUE
    )
    create_table_from_source(
      table_name,
      sprintf("SELECT * FROM %s", quote_ident(temp_view_name, conn)),
      layout, prepared$labels, conn
    )
    return(invisible(NULL))
  }

  # If data_source is a URL, ensure httpfs extension is installed and loaded
  if (is.character(data_source) && grepl("^https?://", data_source)) {
    load_or_install_extension("httpfs")
  }

  # Handle file paths and URLs
  if (is.character(data_source)) {
    create_table_from_source(
      table_name,
      sprintf("SELECT * FROM %s", quote_sql(data_source)),
      layout, character(), conn
    )
  } else {
    cli::cli_abort("{.arg data_source} must be a character string (file path or URL) or a data.frame.")
  }

  invisible(NULL)
}

#' Validate create_table()'s layout arguments before anything is created
#'
#' @returns A list holding the pieces that were given, empty when none.
#' @noRd
check_table_layout <- function(partition_by, sort_by, options) {
  layout <- list()
  if (!is.null(partition_by)) {
    layout$partition_by <- check_partition_by(partition_by)
  }
  if (!is.null(sort_by)) {
    layout$sort_by <- check_sort_by(sort_by)
  }
  if (!is.null(options)) {
    render_option_assignments(options)
    layout$options <- options
  }
  layout
}

#' Create a table from a SELECT, with its layout and labels, as one snapshot
#'
#' Without a layout this is one `CREATE TABLE ... AS`. With one, the
#' sequence DuckLake accepts in a transaction: create the table empty from
#' the source's columns, put the keys and options on, insert the rows.
#' Either way the labels follow in the same transaction, which is the
#' caller's when one is open and this function's own otherwise.
#'
#' @param table_name Name of the new table.
#' @param source_sql A `SELECT` producing the rows.
#' @param layout The list from `check_table_layout()`.
#' @param comments Named character vector of column comments to store.
#' @param conn The package connection.
#' @returns Invisibly, `NULL`.
#' @noRd
create_table_from_source <- function(table_name, source_sql, layout, comments,
                                     conn) {
  quoted <- quote_ident(table_name, conn)
  has_layout <- length(layout) > 0

  own_txn <- (has_layout || length(comments) > 0) && !in_transaction(conn)
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

  if (has_layout) {
    # Keys and options go on the empty table so the first load already
    # lands partitioned, sorted, and inlined (or not) as asked
    db_execute(sprintf("CREATE TABLE %s AS %s LIMIT 0;", quoted, source_sql), conn = conn)
    apply_table_layout(
      table_name,
      partition_by = layout$partition_by,
      sort_by = layout$sort_by,
      options = layout$options,
      conn = conn
    )
    db_execute(sprintf("INSERT INTO %s %s;", quoted, source_sql), conn = conn)
  } else {
    db_execute(sprintf("CREATE TABLE %s AS %s;", quoted, source_sql), conn = conn)
  }
  store_column_labels(table_name, comments, conn)

  if (own_txn) {
    DBI::dbExecute(conn, "COMMIT;")
    committed <- TRUE
  }
  if (length(comments) > 0) {
    dl_inform(
      "Stored {length(comments)} column label{?s} as column comment{?s}."
    )
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
    dl_inform(
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

#' Is a lazy table backed by this connection?
#' @noRd
same_connection <- function(tbl, conn) {
  remote <- tryCatch(dbplyr::remote_con(tbl), error = function(e) NULL)
  !is.null(remote) && identical(remote, conn)
}

#' Create a table from a lazy query, inside DuckDB
#'
#' `CREATE TABLE ... AS` with the rendered dbplyr SQL, so the rows never
#' pass through R. Column comments inherited from the source tables are
#' stored in the same transaction, as the data-frame path does for labels.
#'
#' @param .data A lazy table on `conn`.
#' @param table_name Name of the new table.
#' @param labels Whether to carry source column comments over.
#' @param layout The list from `check_table_layout()`.
#' @param conn The package connection.
#' @returns Invisibly, `NULL`.
#' @noRd
create_table_from_query <- function(.data, table_name, labels, layout, conn) {
  column_comments <- if (isTRUE(labels)) {
    source_column_comments(.data, conn)
  } else {
    character()
  }
  # A subquery, so the layout path can append LIMIT 0 to any rendered SQL
  source_sql <- sprintf(
    "SELECT * FROM (\n%s\n) AS ducklake_source",
    dbplyr::sql_render(.data, conn)
  )
  create_table_from_source(table_name, source_sql, layout, column_comments, conn)
}

#' Column comments a query's output columns inherit from the tables it reads
#'
#' An output column keeps the comment of a same-named column in any base
#' table of the query (first source wins); renamed or derived columns get
#' none. The table a [get_ducklake_table()] pipeline started from is always
#' consulted, even when the built query hides it behind raw SQL (a
#' time-travel read, for example).
#'
#' @param .data A lazy table.
#' @param conn A DBI connection.
#' @returns A named character vector, column name to comment.
#' @noRd
source_column_comments <- function(.data, conn) {
  out_cols <- colnames(.data)

  sources <- attr(.data, "ducklake_table_name", exact = TRUE)
  bases <- tryCatch(
    query_base_tables(dbplyr::sql_build(.data)),
    error = function(e) character()
  )
  bases <- vapply(bases[!is.na(bases)], table_ref_name, character(1))
  sources <- unique(c(sources, bases[!is.na(bases)]))

  comments <- character()
  for (src in sources) {
    found <- tryCatch(get_table_comments(src), error = function(e) NULL)
    if (is.null(found) || nrow(found) == 0) {
      next
    }
    found <- found[
      found$object_type == "column" & found$column_name %in% out_cols &
        !(found$column_name %in% names(comments)), ,
      drop = FALSE
    ]
    comments[found$column_name] <- found$comment
  }
  comments
}

#' Reduce a base-table reference from `query_base_tables()` to a name
#'
#' dbplyr hands back a table path (`cars`, `"main"."cars"`) or, for a
#' dotted name the duckdb driver wrapped in SQL, `FROM main.cars`. Returns
#' the name with its qualification and case intact, or `NA` for anything
#' that is not a plain identifier path.
#' @noRd
table_ref_name <- function(x) {
  x <- gsub('"', "", as.character(x)[[1]])
  x <- sub("^FROM\\s+", "", x)
  if (grepl("^[A-Za-z_][A-Za-z0-9_.]*$", x)) x else NA_character_
}
