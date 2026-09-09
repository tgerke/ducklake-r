#' Capture the metadata DuckLake keys to a table
#'
#' DuckLake stores comments, partition keys, sort order, and table-scoped
#' options against a table id, and a rewrite that goes through `DROP` +
#' `CREATE` ([replace_table()], [restore_table_version()]) assigns a new id.
#' Capture before the rewrite and put the pieces back with
#' `reapply_table_keys()`, `reapply_table_comments()`, and
#' `reapply_table_options()`. A table with none of these captures empty
#' pieces and reapplies nothing.
#'
#' Two DuckLake rules shape the reapply helpers. `ALTER TABLE ... SET
#' PARTITIONED BY / SORTED BY` is refused on a table holding inlined data
#' written in the open transaction, so keys go on the empty table before its
#' rows are inserted. And `set_option()` is refused for any table created in
#' the open transaction, so options are re-set after the commit.
#'
#' @param table_name The table, optionally schema-qualified.
#' @param ducklake_name Lake name, or `NULL` for the current database.
#' @param conn A DBI connection.
#' @returns A list with `table_name`, `ducklake_name`, and the data frames
#'   `comments`, `partitions`, `sorting`, and `options`.
#' @noRd
capture_table_metadata <- function(table_name, ducklake_name = NULL,
                                   conn = get_ducklake_connection()) {
  ducklake_name <- infer_ducklake_name(ducklake_name, conn)
  parts <- split_table_name(table_name)
  schema <- if (is.null(parts$schema)) "main" else parts$schema
  scope_entry <- paste(schema, parts$table, sep = ".")

  options <- get_ducklake_options(ducklake_name)
  options <- options[
    options$scope == "TABLE" & !is.na(options$scope_entry) &
      options$scope_entry == scope_entry, ,
    drop = FALSE
  ]

  list(
    table_name = table_name,
    ducklake_name = ducklake_name,
    comments = get_table_comments(table_name, ducklake_name),
    partitions = get_table_partitions(table_name, ducklake_name),
    sorting = get_table_sorting(table_name, ducklake_name),
    options = options
  )
}

#' Put captured partition and sort keys on a freshly created, still empty table
#'
#' Keys on columns the new table no longer has are dropped. Expression sort
#' keys (set with raw SQL) are attempted as they are; if DuckLake rejects
#' one the error surfaces and the open transaction rolls back, which beats
#' silently losing the key.
#'
#' @param meta The list from `capture_table_metadata()`.
#' @param columns Column names of the new table.
#' @param conn A DBI connection.
#' @returns Invisibly, `NULL`.
#' @noRd
reapply_table_keys <- function(meta, columns, conn = get_ducklake_connection()) {
  quoted <- quote_ident(meta$table_name, conn)

  partitions <- meta$partitions
  partitions <- partitions[partitions$column_name %in% columns, , drop = FALSE]
  if (nrow(partitions) > 0) {
    db_execute(
      sprintf(
        "ALTER TABLE %s SET PARTITIONED BY (%s);",
        quoted, paste(partition_expressions(partitions), collapse = ", ")
      ),
      conn = conn
    )
  }

  sorting <- meta$sorting
  if (nrow(sorting) > 0) {
    is_bare <- grepl("^[A-Za-z_][A-Za-z0-9_]*$", sorting$expression)
    sorting <- sorting[!is_bare | sorting$expression %in% columns, , drop = FALSE]
  }
  if (nrow(sorting) > 0) {
    db_execute(
      sprintf(
        "ALTER TABLE %s SET SORTED BY (%s);",
        quoted, paste(sort_expressions(sorting), collapse = ", ")
      ),
      conn = conn
    )
  }

  invisible(NULL)
}

#' Put captured comments back on the new table
#'
#' Column comments go on columns that still exist, except those this
#' rewrite has already commented (labels stored from a data frame take
#' precedence); the table comment is set again as captured. The catalog is
#' not consulted here: inside the rewrite's transaction it still shows the
#' dropped table's rows, so what was already set has to be passed in.
#'
#' @inheritParams reapply_table_keys
#' @param already_set Column names commented earlier in this rewrite.
#' @returns Invisibly, `NULL`.
#' @noRd
reapply_table_comments <- function(meta, columns, already_set = character(),
                                   conn = get_ducklake_connection()) {
  comments <- meta$comments
  if (nrow(comments) == 0) {
    return(invisible(NULL))
  }
  quoted <- quote_ident(meta$table_name, conn)

  for (i in seq_len(nrow(comments))) {
    type <- comments$object_type[[i]]
    if (type == "table") {
      db_execute(
        sprintf(
          "COMMENT ON TABLE %s IS %s;",
          quoted, quote_sql(comments$comment[[i]])
        ),
        conn = conn
      )
    } else if (type == "column") {
      col <- comments$column_name[[i]]
      if (col %in% columns && !(col %in% already_set)) {
        db_execute(
          sprintf(
            "COMMENT ON COLUMN %s.%s IS %s;",
            quoted, quote_column(col, conn), quote_sql(comments$comment[[i]])
          ),
          conn = conn
        )
      }
    }
  }
  invisible(NULL)
}

#' Re-set captured table-scoped options after the rewrite is committed
#'
#' DuckLake refuses `set_option()` on a table created in the open
#' transaction, so this runs after the commit, in a transaction of its own
#' (one snapshot). Inside a transaction the caller opened it cannot run at
#' all: the options are listed in a warning instead.
#'
#' @param meta The list from `capture_table_metadata()`.
#' @param conn A DBI connection.
#' @returns Invisibly, `NULL`.
#' @noRd
reapply_table_options <- function(meta, conn = get_ducklake_connection()) {
  opts <- meta$options
  if (nrow(opts) == 0) {
    return(invisible(NULL))
  }
  if (in_transaction(conn)) {
    calls <- sprintf(
      'set_ducklake_option("%s", "%s", table_name = "%s")',
      opts$option_name, opts$value, meta$table_name
    )
    cli::cli_warn(c(
      "Table-scoped option{?s} {.val {opts$option_name}} could not be carried over to the rewritten table {.val {meta$table_name}} inside the open transaction.",
      "i" = "DuckLake cannot set options on a table created in the same transaction. After committing, run:",
      stats::setNames(calls, rep(" ", length(calls)))
    ))
    return(invisible(NULL))
  }

  parts <- split_table_name(meta$table_name)
  DBI::dbExecute(conn, "BEGIN TRANSACTION;")
  committed <- FALSE
  on.exit(
    if (!committed) {
      tryCatch(DBI::dbExecute(conn, "ROLLBACK;"), error = function(e) NULL)
    },
    add = TRUE
  )
  for (i in seq_len(nrow(opts))) {
    db_execute(
      sprintf(
        "CALL %s.set_option(%s, %s%s);",
        meta$ducklake_name,
        quote_sql(opts$option_name[[i]]),
        quote_sql(opts$value[[i]]),
        option_scope_args(parts$table, parts$schema)
      ),
      conn = conn
    )
  }
  DBI::dbExecute(conn, "COMMIT;")
  committed <- TRUE
  invisible(NULL)
}

#' Rebuild a table from a copy of its new contents, inside the open transaction
#'
#' The sequence DuckLake accepts in one transaction: drop, create empty
#' with the copy's schema, put the partition and sort keys on, insert, then
#' comment. `source_ref` is a quoted reference to the copy (a registered
#' data-frame view or a temporary table).
#'
#' @param table_name The table to rebuild.
#' @param source_ref Quoted SQL reference to the new contents.
#' @param meta The list from `capture_table_metadata()`.
#' @param comments Named character vector of column comments to store
#'   first (labels from a data frame, or comments inherited from source
#'   tables); captured comments fill the remaining columns.
#' @param conn A DBI connection.
#' @returns Invisibly, the column names of the rebuilt table.
#' @noRd
rebuild_table_from <- function(table_name, source_ref, meta, comments,
                               conn = get_ducklake_connection()) {
  quoted <- quote_ident(table_name, conn)
  columns <- names(DBI::dbGetQuery(
    conn, sprintf("SELECT * FROM %s LIMIT 0", source_ref)
  ))

  db_execute(sprintf("DROP TABLE IF EXISTS %s;", quoted), conn = conn)
  db_execute(
    sprintf("CREATE TABLE %s AS SELECT * FROM %s LIMIT 0;", quoted, source_ref),
    conn = conn
  )
  reapply_table_keys(meta, columns, conn)
  db_execute(
    sprintf("INSERT INTO %s SELECT * FROM %s;", quoted, source_ref),
    conn = conn
  )
  store_column_labels(table_name, comments, conn)
  reapply_table_comments(meta, columns, names(comments), conn)

  invisible(columns)
}

#' Materialize a lazy query into a temporary DuckDB table
#'
#' Used before a rewrite whose query may read the table being replaced:
#' the copy lives in DuckDB's temp catalog (spilling to disk as needed), so
#' nothing passes through R. The caller drops it, typically with
#' `on.exit()`.
#'
#' @param .data A lazy table on `conn`.
#' @param table_name The target table, used to derive the temp name.
#' @param conn A DBI connection.
#' @returns The quoted reference to the temporary table.
#' @noRd
materialize_query <- function(.data, table_name, conn = get_ducklake_connection()) {
  name <- paste0("__ducklake_rewrite_", gsub("[^a-zA-Z0-9]", "_", table_name))
  ref <- quote_ident(paste("temp", "main", name, sep = "."), conn)
  db_execute(sprintf("DROP TABLE IF EXISTS %s;", ref), conn = conn)
  db_execute(
    sprintf(
      "CREATE TEMP TABLE %s AS\n%s;",
      quote_ident(name, conn), dbplyr::sql_render(.data, conn)
    ),
    conn = conn
  )
  ref
}

#' Rebuild `SET PARTITIONED BY` expressions from `get_table_partitions()` rows
#'
#' The catalog stores the transform separately from the column:
#' `identity`, `year`/`month`/`day`/`hour`, or `bucket(N)`.
#' @noRd
partition_expressions <- function(partitions) {
  vapply(
    seq_len(nrow(partitions)),
    function(i) {
      col <- partitions$column_name[[i]]
      transform <- partitions$transform[[i]]
      if (transform == "identity") {
        col
      } else if (grepl("^bucket\\([0-9]+\\)$", transform)) {
        n <- sub("^bucket\\(([0-9]+)\\)$", "\\1", transform)
        sprintf("bucket(%s, %s)", n, col)
      } else {
        sprintf("%s(%s)", transform, col)
      }
    },
    character(1)
  )
}

#' Rebuild `SET SORTED BY` expressions from `get_table_sorting()` rows
#' @noRd
sort_expressions <- function(sorting) {
  direction <- ifelse(is.na(sorting$sort_direction), "", sorting$sort_direction)
  nulls <- ifelse(
    is.na(sorting$null_order), "",
    gsub("_", " ", sorting$null_order, fixed = TRUE)
  )
  trimws(paste(sorting$expression, direction, nulls))
}

#' Every table id a name has had in a schema
#'
#' `replace_table()` and `restore_table_version()` assign new ids, so a
#' table's history spans several. Metadata rows are not filtered on
#' `end_snapshot` on purpose.
#' @noRd
table_ids_for_name <- function(table, schema, ducklake_name,
                               conn = get_ducklake_connection()) {
  prefix <- metadata_prefix(ducklake_name, conn)
  tryCatch(
    DBI::dbGetQuery(
      conn,
      sprintf(
        "SELECT DISTINCT t.table_id
         FROM %s.ducklake_table t
         JOIN %s.ducklake_schema s ON t.schema_id = s.schema_id
         WHERE t.table_name = ? AND s.schema_name = ?",
        prefix, prefix
      ),
      params = list(table, schema)
    )$table_id,
    error = function(e) integer(0)
  )
}

#' The change-map values that identify a table
#'
#' Creates and renames name the table (`schema.table`); row changes,
#' alters, drops, and flushes carry its numeric id, and a renamed or
#' replaced table has had several ids, so every id the name has had in the
#' schema counts.
#'
#' @returns A character vector: the qualified name and the ids as strings.
#' @noRd
table_change_targets <- function(table_name, ducklake_name,
                                 conn = get_ducklake_connection()) {
  parts <- split_table_name(table_name)
  schema <- if (is.null(parts$schema)) "main" else parts$schema
  ids <- table_ids_for_name(parts$table, schema, ducklake_name, conn)
  c(paste0(schema, ".", parts$table), as.character(ids))
}

#' Which snapshots touched a table?
#'
#' Matches the `changes` column of `snapshots()` against the values that
#' identify the table (see table_change_targets()). Handles both the
#' key/value data frames the duckdb driver returns and a plain
#' comma-separated string.
#'
#' @param changes The `changes` column, one element per snapshot.
#' @returns A logical vector, one per snapshot.
#' @noRd
snapshots_touching_table <- function(changes, table_name, ducklake_name,
                                     conn = get_ducklake_connection()) {
  targets <- table_change_targets(table_name, ducklake_name, conn)
  vapply(
    changes,
    function(entry) any(change_values(entry) %in% targets),
    logical(1),
    USE.NAMES = FALSE
  )
}

#' Flatten one snapshot's change map to its values, minus schema entries
#' @noRd
change_values <- function(entry) {
  if (is.data.frame(entry)) {
    keep <- !startsWith(as.character(entry$key), "schemas")
    values <- unlist(entry$value[keep], use.names = FALSE)
  } else {
    values <- trimws(
      strsplit(paste(as.character(entry), collapse = ","), ",", fixed = TRUE)[[1]]
    )
  }
  values <- as.character(values)
  values[!is.na(values)]
}
