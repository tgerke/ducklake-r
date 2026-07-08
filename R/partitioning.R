#' Set partitioning keys for a table
#'
#' Declares how newly written data files for a table should be split up.
#' Partitioning lets DuckLake prune files during query planning, which can
#' speed up filtered reads on large tables considerably.
#'
#' @param table_name The name of the table to partition.
#' @param partition_by Character vector of partition expressions. Each entry
#'   must be one of:
#'   \itemize{
#'     \item a column name, e.g. `"region"` (identity transform)
#'     \item `"year(col)"`, `"month(col)"`, `"day(col)"`, or `"hour(col)"`
#'       for timestamp columns
#'     \item `"bucket(n, col)"` for hash bucketing into `n` buckets
#'   }
#'
#' @details
#' Partitioning only affects data written *after* the keys are set;
#' previously written files keep their layout. To re-partition existing
#' data, rewrite the table (e.g. with [replace_table()]) after setting the
#' keys.
#'
#' Runs `ALTER TABLE ... SET PARTITIONED BY (...)`. The expressions are
#' validated against the transforms DuckLake supports before any SQL is
#' built.
#'
#' @returns Invisibly returns `NULL`.
#' @family partitioning
#' @export
#'
#' @seealso [reset_table_partitioning()], [get_table_partitions()]
#'
#' @examples
#' \dontrun{
#' # Partition new files by year and month of the event timestamp
#' set_table_partitioning("events", c("year(event_time)", "month(event_time)"))
#'
#' # Plain column partitioning
#' set_table_partitioning("sales", "region")
#'
#' # Hash user ids into 8 buckets, then split by month
#' set_table_partitioning("visits", c("bucket(8, user_id)", "month(ts)"))
#' }
set_table_partitioning <- function(table_name, partition_by) {
  conn <- get_ducklake_connection()

  if (!is.character(partition_by) || length(partition_by) == 0 ||
      anyNA(partition_by)) {
    cli::cli_abort(
      "{.arg partition_by} must be a character vector of partition expressions."
    )
  }

  ident <- "[A-Za-z_][A-Za-z0-9_]*"
  allowed <- c(
    sprintf("^%s$", ident),
    sprintf("^(year|month|day|hour)\\(\\s*%s\\s*\\)$", ident),
    sprintf("^bucket\\(\\s*[0-9]+\\s*,\\s*%s\\s*\\)$", ident)
  )
  ok <- vapply(
    partition_by,
    function(p) any(vapply(allowed, grepl, logical(1), x = p)),
    logical(1)
  )
  if (!all(ok)) {
    cli::cli_abort(c(
      "Invalid partition expression{?s}: {.val {partition_by[!ok]}}.",
      "i" = "Supported forms: a column name, {.code year/month/day/hour(col)}, or {.code bucket(n, col)}."
    ))
  }

  db_execute(
    sprintf(
      "ALTER TABLE %s SET PARTITIONED BY (%s);",
      quote_ident(table_name, conn),
      paste(partition_by, collapse = ", ")
    ),
    conn = conn
  )
  cli::cli_inform(c(
    "Table {.val {table_name}} is now partitioned by {.val {partition_by}}.",
    "i" = "Only newly written data is partitioned; existing files keep their layout."
  ))

  invisible(NULL)
}

#' Remove partitioning keys from a table
#'
#' Clears a table's partitioning keys so newly written data files are no
#' longer split along them. Existing files are unaffected.
#'
#' @param table_name The name of the table.
#'
#' @returns Invisibly returns `NULL`.
#' @family partitioning
#' @export
#'
#' @seealso [set_table_partitioning()], [get_table_partitions()]
#'
#' @examples
#' \dontrun{
#' reset_table_partitioning("events")
#' }
reset_table_partitioning <- function(table_name) {
  conn <- get_ducklake_connection()

  db_execute(
    sprintf(
      "ALTER TABLE %s RESET PARTITIONED BY;",
      quote_ident(table_name, conn)
    ),
    conn = conn
  )
  cli::cli_inform("Partitioning removed from table {.val {table_name}}.")

  invisible(NULL)
}

#' List the partitioning keys of tables in a lake
#'
#' Reads the current partitioning keys from the DuckLake metadata catalog.
#'
#' @param table_name Optional table name to filter to a single table.
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @returns A data frame with one row per partition key: `table_name`,
#'   `partition_key_index`, `column_name`, and `transform` (e.g.
#'   `"identity"` or `"year"`). Zero rows when nothing is partitioned.
#' @family partitioning
#' @export
#'
#' @seealso [set_table_partitioning()], [get_metadata_table()]
#'
#' @examples
#' \dontrun{
#' # All partitioned tables in the lake
#' get_table_partitions()
#'
#' # Keys for one table
#' get_table_partitions("events")
#' }
get_table_partitions <- function(table_name = NULL, ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  # Metadata tables live in the __ducklake_metadata_[name] database.
  # DuckDB and SQLite use a .main. schema qualifier; PostgreSQL and MySQL do not.
  meta_db <- paste0("__ducklake_metadata_", ducklake_name)
  prefix <- if (get_ducklake_backend() %in% c("postgres", "mysql")) {
    quote_ident(meta_db, conn)
  } else {
    paste0(quote_ident(meta_db, conn), ".main")
  }

  filter_clause <- if (is.null(table_name)) "" else "AND t.table_name = ?"

  # Current (non-superseded) metadata rows have end_snapshot IS NULL
  sql <- sprintf(
    "SELECT t.table_name, pc.partition_key_index, c.column_name, pc.transform
     FROM %s.ducklake_partition_info pi
     JOIN %s.ducklake_partition_column pc
       ON pi.partition_id = pc.partition_id AND pi.table_id = pc.table_id
     JOIN %s.ducklake_table t
       ON pi.table_id = t.table_id AND t.end_snapshot IS NULL
     JOIN %s.ducklake_column c
       ON pc.column_id = c.column_id AND pc.table_id = c.table_id
       AND c.end_snapshot IS NULL
     WHERE pi.end_snapshot IS NULL %s
     ORDER BY t.table_name, pc.partition_key_index",
    prefix, prefix, prefix, prefix, filter_clause
  )

  if (is.null(table_name)) {
    DBI::dbGetQuery(conn, sql)
  } else {
    DBI::dbGetQuery(conn, sql, params = list(table_name))
  }
}
