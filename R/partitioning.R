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
#' data, set the keys and then rewrite the table with [replace_table()]:
#' the rewrite carries the keys over to the new table and writes every row
#' through them.
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
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("part_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("part_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' # Plain column partitioning
#' set_table_partitioning("cars", "cyl")
#'
#' # Compound key
#' set_table_partitioning("cars", c("gear", "cyl"))
#'
#' # Timestamp columns can be split by year(), month(), day(), or hour()
#'
#' detach_ducklake("part_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
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
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("unpart_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("unpart_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' set_table_partitioning("cars", "cyl")
#' reset_table_partitioning("cars")
#'
#' detach_ducklake("unpart_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
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
#' @param table_name Optional table name to filter to a single table,
#'   optionally qualified as `"schema.table"`.
#' @param ducklake_name Optional name of the attached DuckLake catalog. If
#'   `NULL`, the current database is used.
#'
#' @returns A data frame with one row per partition key: `schema_name`,
#'   `table_name`, `partition_key_index`, `column_name`, and `transform`
#'   (e.g. `"identity"`, `"year"`, or `"bucket(4)"`). Zero rows when
#'   nothing is partitioned.
#' @family partitioning
#' @export
#'
#' @seealso [set_table_partitioning()], [get_metadata_table()]
#'
#' @examplesIf ducklake_extension_available()
#' lake_dir <- tempfile("getpart_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("getpart_lake", lake_path = lake_dir)
#' create_table(mtcars, "cars")
#'
#' set_table_partitioning("cars", "cyl")
#'
#' # All partitioned tables in the lake
#' get_table_partitions()
#'
#' # Keys for one table
#' get_table_partitions("cars")
#'
#' detach_ducklake("getpart_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
get_table_partitions <- function(table_name = NULL, ducklake_name = NULL) {
  conn <- get_ducklake_connection()
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)

  prefix <- metadata_prefix(ducklake_name, conn)

  filter <- table_filter(table_name)

  # Current (non-superseded) metadata rows have end_snapshot IS NULL
  sql <- sprintf(
    "SELECT s.schema_name, t.table_name, pc.partition_key_index,
            c.column_name, pc.transform
     FROM %1$s.ducklake_partition_info pi
     JOIN %1$s.ducklake_partition_column pc
       ON pi.partition_id = pc.partition_id AND pi.table_id = pc.table_id
     JOIN %1$s.ducklake_table t
       ON pi.table_id = t.table_id AND t.end_snapshot IS NULL
     JOIN %1$s.ducklake_schema s
       ON t.schema_id = s.schema_id AND s.end_snapshot IS NULL
     JOIN %1$s.ducklake_column c
       ON pc.column_id = c.column_id AND pc.table_id = c.table_id
       AND c.end_snapshot IS NULL
     WHERE pi.end_snapshot IS NULL %2$s
     ORDER BY s.schema_name, t.table_name, pc.partition_key_index",
    prefix, filter$sql
  )

  if (length(filter$params) == 0) {
    DBI::dbGetQuery(conn, sql)
  } else {
    DBI::dbGetQuery(conn, sql, params = filter$params)
  }
}
