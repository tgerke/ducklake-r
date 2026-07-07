#' Set the data inlining row limit
#'
#' Controls the threshold below which DuckLake stores small inserts and
#' deletes directly in the catalog database instead of writing Parquet files.
#' This avoids the "small files problem" common in streaming or frequent-update
#' workloads.
#'
#' The limit can be set at three levels (highest priority first):
#' \enumerate{
#'   \item **Table-level** – persisted in the DuckLake metadata for a specific table
#'   \item **Schema-level** – persisted for all tables in a schema
#'   \item **Global (DuckDB setting)** – applies to all DuckLake connections
#' }
#'
#' @param limit Integer. The maximum number of rows that will be inlined.
#'   Set to `0` to disable inlining entirely.
#' @param table_name Optional table name. When provided the limit is persisted
#'   for that table in the DuckLake metadata (takes priority over the global
#'   setting).
#' @param schema_name Optional schema name. When provided (without
#'   `table_name`) the limit is persisted for all tables in that schema.
#' @param ducklake_name Optional name of the attached DuckLake catalog.
#'   Required when setting a table- or schema-level override. If `NULL`, the
#'   current database is used.
#'
#' @details
#' Data inlining is enabled by default in DuckLake v1.0 with a threshold of 10
#' rows. Any insert or delete affecting fewer rows than the limit is written to
#' an inlined table inside the catalog instead of creating a Parquet file.
#'
#' For streaming or high-frequency-insert workloads, increase the limit (e.g.,
#' 50 or 100). For workloads that always write large batches, the default is
#' fine or you can disable inlining with `limit = 0`.
#'
#' Use [flush_inlined_data()] or [checkpoint_ducklake()] to materialise inlined
#' data to Parquet when ready.
#'
#' @returns Invisibly returns `NULL`.
#' @export
#'
#' @seealso [get_inlining_row_limit()], [flush_inlined_data()],
#'   [checkpoint_ducklake()]
#'
#' @examples
#' \dontrun{
#' # Change the global default
#' set_inlining_row_limit(50)
#'
#' # Override for a specific table
#' set_inlining_row_limit(100, table_name = "readings")
#'
#' # Disable inlining globally
#' set_inlining_row_limit(0)
#' }
set_inlining_row_limit <- function(limit,
                                   table_name = NULL,
                                   schema_name = NULL,
                                   ducklake_name = NULL) {
  limit <- as.integer(limit)
  if (is.na(limit) || limit < 0L) {
    cli::cli_abort("{.arg limit} must be a non-negative integer.")
  }

  conn <- get_ducklake_connection()

  if (is.null(table_name) && is.null(schema_name)) {
    # Global DuckDB setting
    DBI::dbExecute(
      conn,
      sprintf("SET ducklake_default_data_inlining_row_limit = %d;", limit)
    )
    cli::cli_inform(
      "Global data inlining row limit set to {.val {limit}}."
    )
  } else {
    # Per-table or per-schema persistent override via CALL set_option()
    if (is.null(ducklake_name)) {
      ducklake_name <- tryCatch(
        DBI::dbGetQuery(conn, "SELECT current_database() AS db")$db,
        error = function(e) NULL
      )
      if (is.null(ducklake_name) || ducklake_name == "") {
        cli::cli_abort(
          "Could not determine {.arg ducklake_name}. Please provide it explicitly."
        )
      }
    }

    if (!is.null(table_name) && !is.null(schema_name)) {
      call_sql <- sprintf(
        "CALL %s.set_option('data_inlining_row_limit', %d, schema => '%s', table_name => '%s');",
        ducklake_name, limit,
        gsub("'", "''", schema_name),
        gsub("'", "''", table_name)
      )
    } else if (!is.null(table_name)) {
      call_sql <- sprintf(
        "CALL %s.set_option('data_inlining_row_limit', %d, table_name => '%s');",
        ducklake_name, limit,
        gsub("'", "''", table_name)
      )
    } else {
      call_sql <- sprintf(
        "CALL %s.set_option('data_inlining_row_limit', %d, schema => '%s');",
        ducklake_name, limit,
        gsub("'", "''", schema_name)
      )
    }

    DBI::dbExecute(conn, call_sql)

    scope <- if (!is.null(table_name)) {
      paste0("table {.val ", table_name, "}")
    } else {
      paste0("schema {.val ", schema_name, "}")
    }
    cli::cli_inform(
      "Data inlining row limit for {scope} set to {.val {limit}}."
    )
  }

  invisible(NULL)
}

#' Get the current data inlining row limit
#'
#' Returns the effective data inlining row limit. When no table- or schema-level
#' override is configured, the global DuckDB default is returned.
#'
#' @param table_name Optional table name to query the table-level override.
#' @param schema_name Optional schema name to query the schema-level override.
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @returns An integer: the effective inlining row limit.
#' @export
#'
#' @seealso [set_inlining_row_limit()]
#'
#' @examples
#' \dontrun{
#' # Global default
#' get_inlining_row_limit()
#'
#' # Table-specific limit
#' get_inlining_row_limit(table_name = "readings")
#' }
get_inlining_row_limit <- function(table_name = NULL,
                                   schema_name = NULL,
                                   ducklake_name = NULL) {
  conn <- get_ducklake_connection()

  if (is.null(table_name) && is.null(schema_name)) {
    result <- DBI::dbGetQuery(
      conn,
      "SELECT value FROM duckdb_settings() WHERE name = 'ducklake_default_data_inlining_row_limit';"
    )
    if (nrow(result) == 0L) {
      return(10L)
    }
    return(as.integer(result$value[1L]))
  }

  if (is.null(ducklake_name)) {
    ducklake_name <- tryCatch(
      DBI::dbGetQuery(conn, "SELECT current_database() AS db")$db,
      error = function(e) NULL
    )
    if (is.null(ducklake_name) || ducklake_name == "") {
      cli::cli_abort(
        "Could not determine {.arg ducklake_name}. Please provide it explicitly."
      )
    }
  }

  if (!is.null(table_name) && !is.null(schema_name)) {
    scope_filter <- sprintf("scope = 'TABLE' AND scope_entry = '%s.%s'",
                            gsub("'", "''", schema_name),
                            gsub("'", "''", table_name))
  } else if (!is.null(table_name)) {
    scope_filter <- sprintf("scope = 'TABLE' AND scope_entry LIKE '%%.%s'",
                            gsub("'", "''", table_name))
  } else {
    scope_filter <- sprintf("scope = 'SCHEMA' AND scope_entry = '%s'",
                            gsub("'", "''", schema_name))
  }

  call_sql <- sprintf(
    "SELECT value FROM ducklake_options('%s') WHERE option_name = 'data_inlining_row_limit' AND %s;",
    ducklake_name, scope_filter
  )

  result <- tryCatch(
    DBI::dbGetQuery(conn, call_sql),
    error = function(e) {
      cli::cli_warn("Could not query per-table inlining limit: {e$message}")
      return(data.frame())
    }
  )

  if (nrow(result) == 0L) {
    # Fall back to global default
    return(get_inlining_row_limit())
  }

  as.integer(result$value[1L])
}

#' Flush inlined data to Parquet files
#'
#' Materialises data that has been stored inline in the catalog database into
#' Parquet files on the data path. This includes both inlined inserts and
#' inlined deletions.
#'
#' @param ducklake_name Name of the attached DuckLake catalog. If `NULL`, the
#'   current database is used.
#' @param table_name Optional table name. When provided, only flushes inlined
#'   data for that table.
#' @param schema_name Optional schema name. When provided, only flushes inlined
#'   data for tables in that schema.
#'
#' @details
#' Flushing writes inlined rows to consolidated Parquet files and cleans up the
#' inlined data tables. Time-travel information is preserved: flushed rows that
#' had been deleted will produce a partial deletion file with snapshot metadata.
#'
#' Tables with `auto_compact` set to `FALSE` are skipped when flushing an
#' entire lake or schema. Use an explicit `table_name` to flush those tables.
#'
#' If a table has a sort order defined, the flushed Parquet file will be sorted
#' by those keys.
#'
#' @returns A data frame with columns `schema_name`, `table_name`, and
#'   `rows_flushed`. Tables with no inlined data are omitted.
#' @export
#'
#' @seealso [set_inlining_row_limit()], [checkpoint_ducklake()]
#'
#' @examples
#' \dontrun{
#' # Flush everything
#' flush_inlined_data()
#'
#' # Flush a specific table
#' flush_inlined_data(table_name = "readings")
#'
#' # Flush a specific schema
#' flush_inlined_data(schema_name = "staging")
#' }
flush_inlined_data <- function(ducklake_name = NULL,
                               table_name = NULL,
                               schema_name = NULL) {
  conn <- get_ducklake_connection()

  if (is.null(ducklake_name)) {
    ducklake_name <- tryCatch(
      DBI::dbGetQuery(conn, "SELECT current_database() AS db")$db,
      error = function(e) NULL
    )
    if (is.null(ducklake_name) || ducklake_name == "") {
      cli::cli_abort(
        "Could not determine {.arg ducklake_name}. Please provide it explicitly."
      )
    }
  }

  # Build the CALL statement
  if (!is.null(table_name) && !is.null(schema_name)) {
    call_sql <- sprintf(
      "SELECT * FROM ducklake_flush_inlined_data('%s', schema_name => '%s', table_name => '%s');",
      ducklake_name,
      gsub("'", "''", schema_name),
      gsub("'", "''", table_name)
    )
  } else if (!is.null(table_name)) {
    call_sql <- sprintf(
      "SELECT * FROM ducklake_flush_inlined_data('%s', table_name => '%s');",
      ducklake_name,
      gsub("'", "''", table_name)
    )
  } else if (!is.null(schema_name)) {
    call_sql <- sprintf(
      "SELECT * FROM ducklake_flush_inlined_data('%s', schema_name => '%s');",
      ducklake_name,
      gsub("'", "''", schema_name)
    )
  } else {
    call_sql <- sprintf(
      "SELECT * FROM ducklake_flush_inlined_data('%s');",
      ducklake_name
    )
  }

  result <- DBI::dbGetQuery(conn, call_sql)

  if (nrow(result) > 0L) {
    total <- sum(result$rows_flushed)
    n_tables <- nrow(result)
    cli::cli_inform(
      "Flushed {.val {total}} row{?s} from {.val {n_tables}} table{?s} to Parquet."
    )
  } else {
    cli::cli_inform("No inlined data to flush.")
  }

  result
}

#' Run a DuckLake checkpoint
#'
#' Runs all maintenance operations on the DuckLake catalog: flushes inlined
#' data, expires old snapshots, merges small files, and cleans up unreferenced
#' files.
#'
#' @param ducklake_name Name of the attached DuckLake catalog. If `NULL`, the
#'   current database is used.
#'
#' @details
#' `CHECKPOINT` is the recommended one-stop maintenance command. It internally
#' calls [flush_inlined_data()] along with compaction, snapshot expiration, and
#' file cleanup.
#'
#' Run checkpoints periodically (e.g., after a batch of streaming inserts) to
#' consolidate inlined data and keep query performance optimal.
#'
#' @returns Invisibly returns `NULL`.
#' @export
#'
#' @seealso [flush_inlined_data()], [set_inlining_row_limit()]
#'
#' @examples
#' \dontrun{
#' # Run all maintenance
#' checkpoint_ducklake()
#'
#' # Or specify a named lake
#' checkpoint_ducklake("my_lake")
#' }
checkpoint_ducklake <- function(ducklake_name = NULL) {
  conn <- get_ducklake_connection()

  if (is.null(ducklake_name)) {
    ducklake_name <- tryCatch(
      DBI::dbGetQuery(conn, "SELECT current_database() AS db")$db,
      error = function(e) NULL
    )
    if (is.null(ducklake_name) || ducklake_name == "") {
      cli::cli_abort(
        "Could not determine {.arg ducklake_name}. Please provide it explicitly."
      )
    }
  }

  DBI::dbExecute(conn, sprintf("CHECKPOINT %s;", ducklake_name))
  cli::cli_inform("Checkpoint completed for {.val {ducklake_name}}.")

  invisible(NULL)
}
