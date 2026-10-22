#' Execute a SQL statement on the shared ducklake connection
#'
#' Thin wrapper around [DBI::dbExecute()] against
#' [get_ducklake_connection()], used by every function in the package that
#' runs a statement for its side effects.
#'
#' @param sql A single SQL statement.
#' @param conn A DBI connection; defaults to the shared ducklake connection.
#' @returns The number of rows affected, invisibly.
#' @noRd
db_execute <- function(sql, conn = get_ducklake_connection()) {
  invisible(DBI::dbExecute(conn, sql))
}

#' Load a DuckDB extension, installing it first if needed
#'
#' This is the package's one install path: the duckdb R client turns
#' DuckDB's automatic extension install off, so a failed `LOAD` never
#' downloads by itself. Says so before installing: a download into DuckDB's
#' extension directory should never be a silent side effect of calling an
#' unrelated function. The message names the directory and warns when it is
#' a temporary one.
#'
#' @param ext Extension name.
#' @param conn A DBI connection; defaults to the shared ducklake connection.
#' @returns Invisibly, `NULL`.
#' @noRd
load_or_install_extension <- function(ext, conn = get_ducklake_connection()) {
  tryCatch(
    db_execute(sprintf("LOAD %s;", ext), conn = conn),
    error = function(e) {
      dir <- extension_directory(conn)
      cli::cli_inform(c(
        "Installing the {.pkg {ext}} DuckDB extension into {.path {dir}}.",
        extension_persistence_hint(dir)
      ))
      db_execute(sprintf("INSTALL %s;", ext), conn = conn)
      db_execute(sprintf("LOAD %s;", ext), conn = conn)
      if (identical(ext, "ducklake")) {
        reset_extension_available_cache()
      }
    }
  )
  invisible(NULL)
}

#' Does a path have a URI scheme (s3://, https://, gs://, ...)?
#'
#' @param path A single path string.
#' @returns `TRUE` for remote URIs, `FALSE` for local paths.
#' @noRd
is_remote_path <- function(path) {
  grepl("^[A-Za-z][A-Za-z0-9+.-]*://", path)
}

#' Quote a (possibly schema-qualified) identifier for SQL
#'
#' Splits `x` on `.` and quotes each part with [DBI::dbQuoteIdentifier()],
#' so `main.my_table` becomes `"main"."my_table"`. Use for table, schema,
#' and database names that end up interpolated into SQL text.
#'
#' @param x A single identifier, optionally qualified with `.`.
#' @param conn A DBI connection used for quoting rules.
#' @returns A quoted identifier string.
#' @noRd
quote_ident <- function(x, conn = get_ducklake_connection()) {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    cli::cli_abort("Identifier must be a single, non-empty string.")
  }
  parts <- strsplit(x, ".", fixed = TRUE)[[1]]
  quoted <- vapply(
    parts,
    function(p) as.character(DBI::dbQuoteIdentifier(conn, p)),
    character(1)
  )
  paste(quoted, collapse = ".")
}

#' Validate a bare SQL identifier
#'
#' Some statements (`CALL lake.set_option(...)`, `CHECKPOINT lake`) don't
#' accept arbitrary quoted identifiers cleanly, so names used there must be
#' plain identifiers. Aborts with a clear message otherwise.
#'
#' @param x The identifier to validate.
#' @param arg Argument name for the error message.
#' @returns `x`, invisibly.
#' @noRd
check_identifier <- function(x, arg = "ducklake_name") {
  if (!is.character(x) || length(x) != 1 || is.na(x) ||
      !grepl("^[A-Za-z_][A-Za-z0-9_]*$", x)) {
    cli::cli_abort(c(
      "{.arg {arg}} must be a simple identifier (letters, digits, and underscores, starting with a letter or underscore).",
      "x" = "Got {.val {x}}."
    ))
  }
  invisible(x)
}

#' Resolve the target DuckLake catalog name
#'
#' Uses the supplied name, or falls back to the currently USEd database.
#' Aborts when neither is available, and validates the result with
#' `check_identifier()` since the CALL statements it feeds require bare
#' identifiers.
#'
#' @param ducklake_name A catalog name, or `NULL` to infer it.
#' @param conn A DBI connection.
#' @returns The resolved catalog name.
#' @noRd
infer_ducklake_name <- function(ducklake_name = NULL,
                                conn = get_ducklake_connection()) {
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
  check_identifier(ducklake_name)
  ducklake_name
}

#' Format a timestamp argument for SQL interpolation
#'
#' POSIXct values are rendered in UTC, because the duckdb driver reads naive
#' timestamp literals as UTC (and returns snapshot times as UTC-tagged
#' POSIXct). Rendering in local time would silently shift the instant by the
#' UTC offset. Character input is passed through and must already be UTC.
#'
#' @param x A POSIXct or character timestamp.
#' @returns A character timestamp.
#' @noRd
format_timestamp <- function(x) {
  if (inherits(x, "POSIXct")) {
    # Rendered to the nearest microsecond, DuckLake's precision. %OS6 would
    # truncate the binary fraction, which puts the literal a microsecond
    # before the snapshot it was read from about half the time, so a
    # snapshot's own time would miss that snapshot
    micros <- round(as.numeric(x) * 1e6)
    whole <- as.POSIXct(micros %/% 1e6, origin = "1970-01-01", tz = "UTC")
    out <- sprintf(
      "%s.%06d",
      format(whole, "%Y-%m-%d %H:%M:%S", tz = "UTC"),
      as.integer(micros %% 1e6)
    )
    out[is.na(x)] <- NA_character_
    out
  } else {
    as.character(x)
  }
}

#' Refuse a time-travel timestamp that lies in the future
#'
#' DuckLake 1.1 rejects time travel to a future timestamp, and through
#' dbplyr that error surfaces as "Can't query fields" when the lazy table is
#' first touched. Checking in R first gives the message at the call. A
#' character timestamp is read as UTC, the way DuckLake reads the literal;
#' one that does not parse is left for DuckLake to refuse.
#'
#' @param x A POSIXct or character timestamp.
#' @param arg The argument name, for the message.
#' @returns Invisibly, `x`.
#' @noRd
check_not_future <- function(x, arg = "timestamp") {
  instant <- if (inherits(x, "POSIXct")) {
    x
  } else {
    tryCatch(as.POSIXct(x, tz = "UTC"), error = function(e) NA)
  }
  if (length(instant) == 1 && !is.na(instant) && instant > Sys.time()) {
    cli::cli_abort(c(
      "{.arg {arg}} is in the future: {format_timestamp(instant)} UTC.",
      "i" = "DuckLake reads committed snapshots, so a time-travel timestamp must be no later than now ({format_timestamp(Sys.time())} UTC)."
    ))
  }
  invisible(x)
}

#' Test whether the connection has an open transaction
#'
#' DuckDB aborts an open transaction when any statement in it fails, so
#' probing with a BEGIN would poison a caller's transaction. Instead compare
#' `current_transaction_id()` across two statements: in autocommit mode each
#' statement runs in its own transaction so the id advances, while inside an
#' open transaction it stays the same.
#'
#' @param conn A DBI connection.
#' @returns `TRUE` if a transaction is open, otherwise `FALSE`.
#' @noRd
in_transaction <- function(conn = get_ducklake_connection()) {
  !is.na(open_transaction_id(conn))
}

#' The id of the connection's open transaction
#'
#' Read the way `in_transaction()` describes. The id is constant while the
#' transaction is open and is not handed out again, so it identifies the
#' transaction to anything that has to be forgotten when it ends.
#'
#' @param conn A DBI connection.
#' @returns The transaction id, or `NA` when no transaction is open.
#' @noRd
open_transaction_id <- function(conn = get_ducklake_connection()) {
  ids <- tryCatch(
    c(
      DBI::dbGetQuery(conn, "SELECT current_transaction_id() AS id")$id,
      DBI::dbGetQuery(conn, "SELECT current_transaction_id() AS id")$id
    ),
    error = function(e) NULL
  )
  if (length(ids) == 2 && ids[1] == ids[2]) ids[[1]] else NA
}

#' Quote a value as a SQL string literal
#'
#' Wraps `x` in single quotes and doubles any embedded single quotes.
#'
#' @param x A length-one character vector.
#' @returns A quoted SQL string literal.
#' @keywords internal
quote_sql <- function(x) {
  sprintf("'%s'", gsub("'", "''", x))
}

#' Quote a single column name
#'
#' Unlike `quote_ident()`, does not split on `.`, so column names such as
#' `Sepal.Length` stay one identifier.
#'
#' @param x A single column name.
#' @param conn A DBI connection used for quoting rules.
#' @param arg Argument name for error messages.
#' @returns A quoted identifier string.
#' @noRd
quote_column <- function(x, conn = get_ducklake_connection(),
                         arg = "column_name") {
  if (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x)) {
    cli::cli_abort("{.arg {arg}} must be a single, non-empty column name.")
  }
  as.character(DBI::dbQuoteIdentifier(conn, x))
}

#' Render an R value as a SQL literal for a DEFAULT clause
#'
#' DuckLake stores column defaults as plain constants and rejects anything
#' else as "non-literal": `DEFAULT TRUE`, `DATE '...'`, `TIMESTAMP '...'`,
#' and casts all fail, while a bare number or a quoted string is accepted
#' and converted to the column type. So logicals, Dates, and POSIXct values
#' (in UTC) render as quoted strings; `NA` renders as `NULL`.
#'
#' @param x A length-one vector.
#' @returns A SQL literal string.
#' @noRd
render_sql_literal <- function(x) {
  if (length(x) != 1) {
    cli::cli_abort("SQL literal values must have length 1.")
  }
  if (is.na(x)) {
    "NULL"
  } else if (inherits(x, "Date")) {
    quote_sql(format(x, "%Y-%m-%d"))
  } else if (inherits(x, "POSIXct")) {
    quote_sql(format_timestamp(x))
  } else if (is.logical(x)) {
    if (x) "'true'" else "'false'"
  } else if (is.numeric(x)) {
    format(x, scientific = FALSE)
  } else if (is.character(x)) {
    quote_sql(x)
  } else {
    cli::cli_abort("Cannot render {.cls {class(x)}} as a SQL literal.")
  }
}

#' Run a DDL statement, rolling back an aborted autocommit transaction
#'
#' Some DuckLake DDL failures (an unsupported DEFAULT, for one) leave an
#' aborted transaction behind even in autocommit mode, and every later
#' statement on the connection then fails with "Current transaction is
#' aborted (please ROLLBACK)". When no transaction was open beforehand,
#' roll back on failure before re-raising; inside a caller's transaction the
#' caller owns the rollback.
#'
#' @param sql A single SQL statement.
#' @param conn A DBI connection.
#' @returns The number of rows affected, invisibly.
#' @noRd
db_execute_ddl <- function(sql, conn = get_ducklake_connection()) {
  own <- !in_transaction(conn)
  tryCatch(
    db_execute(sql, conn = conn),
    error = function(e) {
      if (own) {
        try(DBI::dbExecute(conn, "ROLLBACK;"), silent = TRUE)
      }
      stop(e)
    }
  )
}

#' Split a possibly qualified table name into lake, schema path, and table
#'
#' Every dot is a path separator inside one lake: `"cars"` gives
#' `list(lake = NULL, schema = NULL, table = "cars")`, `"bronze.cars"` the
#' schema `"bronze"`, and `"sales.europe.orders"` the nested schema path
#' `"sales.europe"`. A first component naming an attached lake (one in the
#' registry) is the lake and is stripped from the path, so
#' `"lake.sales.orders"` resolves in `lake`; a schema that shares an
#' attached lake's name is reached by qualifying with the lake. Nothing is
#' quoted here.
#'
#' @param table_name A table name, optionally qualified with `.`.
#' @returns A list with `lake` and `schema` (`NULL` when absent) and
#'   `table`.
#' @noRd
split_table_name <- function(table_name) {
  if (!is.character(table_name) || length(table_name) != 1 || is.na(table_name)) {
    cli::cli_abort("{.arg table_name} must be a single string.")
  }
  parts <- strsplit(table_name, ".", fixed = TRUE)[[1]]
  if (length(parts) == 0 || any(!nzchar(parts))) {
    cli::cli_abort(
      "{.arg table_name} has an empty name component: {.val {table_name}}."
    )
  }
  lake <- NULL
  if (length(parts) >= 2 && parts[[1]] %in% names(.ducklake_env$lakes)) {
    lake <- parts[[1]]
    parts <- parts[-1]
  }
  n <- length(parts)
  list(
    lake = lake,
    schema = if (n >= 2) paste(parts[-n], collapse = ".") else NULL,
    table = parts[[n]]
  )
}

#' Quoted prefix of a lake's metadata catalog for SQL text
#'
#' A lake attached with `metadata_schema` keeps its DuckLake tables in that
#' schema, which the lake's registry entry remembers: the metadata database
#' is hidden from DuckDB's catalog functions, so the schema cannot be read
#' back later. Otherwise DuckDB and SQLite catalogs keep the tables in a
#' `main` schema, and PostgreSQL and MySQL catalogs expose them at the top
#' level. Resolves the backend from the registry entry, not the current
#' database.
#'
#' @param ducklake_name The lake name.
#' @param conn A DBI connection used for quoting rules.
#' @returns A string such as `__ducklake_metadata_lake.main`, each part
#'   quoted when its spelling calls for it.
#' @noRd
metadata_prefix <- function(ducklake_name, conn = get_ducklake_connection()) {
  meta_db <- quote_ident(paste0("__ducklake_metadata_", ducklake_name), conn)
  schema <- .ducklake_env$lakes[[ducklake_name]]$metadata_schema
  if (!is.null(schema)) {
    return(paste0(meta_db, ".", quote_ident(schema, conn)))
  }
  if (get_ducklake_backend(ducklake_name) %in% c("postgres", "mysql")) {
    meta_db
  } else {
    paste0(meta_db, ".main")
  }
}

#' Where DuckDB keeps downloaded extensions for this connection
#'
#' @param conn A DBI connection.
#' @returns The directory as a string, or `NA` if it cannot be read.
#' @noRd
extension_directory <- function(conn = get_ducklake_connection()) {
  tryCatch(
    DBI::dbGetQuery(
      conn, "SELECT current_setting('extension_directory') AS d"
    )$d,
    error = function(e) NA_character_
  )
}

#' cli bullets explaining how to keep extensions between sessions
#'
#' duckdb resolves a "home" directory for extensions (`duckdb.home` option,
#' `DUCKDB_R_HOME`, an existing `~/.duckdb`, else a per-session temporary
#' directory; see `?duckdb::duckdb_storage`). An install into the temporary
#' directory is repeated on every session, so say so. The check looks at the
#' directory the connection in use reports rather than at
#' `duckdb_storage_status()`, because a user-supplied connection may have
#' been created with its own `home`.
#'
#' @param dir The extension directory, as reported by DuckDB.
#' @returns A named character vector of bullets, empty when the directory
#'   is durable or unknown.
#' @noRd
extension_persistence_hint <- function(dir) {
  if (length(dir) != 1 || is.na(dir) || !nzchar(dir)) {
    return(character())
  }
  # normalizePath() leaves a path that does not exist yet untouched (DuckDB
  # creates the directory on INSTALL), and on macOS tempdir() is a symlinked
  # path with a doubled slash, so compare raw and resolved forms of both
  clean <- function(p) {
    p <- c(p, normalizePath(p, winslash = "/", mustWork = FALSE))
    gsub("/{2,}", "/", gsub("\\\\", "/", p))
  }
  is_temp <- any(outer(clean(dir), clean(tempdir()), startsWith))
  if (!is_temp) {
    return(character())
  }
  c(
    "!" = "This directory is temporary: the extension is gone when the R session ends.",
    "i" = "To keep extensions between sessions, create {.path ~/.duckdb} once (duckdb offers to create it the first time it connects in an interactive session), or set {.envvar DUCKDB_R_HOME} in {.file ~/.Renviron} for scripts and CI. See {.topic duckdb::duckdb_storage}."
  )
}

#' Resolve a table name and an optional schema argument to their parts
#'
#' Functions that take `table_name` and `schema_name` accept either a bare
#' name plus a schema or a qualified `"schema.table"` name. A qualified name
#' fills a `NULL` schema argument; naming two different schemas is an
#' error.
#'
#' @param table_name A table name, optionally qualified.
#' @param schema_name A schema name, or `NULL`.
#' @returns A list with `schema` (`NULL` when neither was given) and
#'   `table`.
#' @noRd
resolve_table_ref <- function(table_name, schema_name = NULL) {
  parts <- split_table_name(table_name)
  if (!is.null(parts$schema) && !is.null(schema_name) &&
      !identical(parts$schema, schema_name)) {
    cli::cli_abort(c(
      "{.arg table_name} names schema {.val {parts$schema}} but {.arg schema_name} is {.val {schema_name}}.",
      "i" = "Give the schema once, either in the name or in {.arg schema_name}."
    ))
  }
  list(
    lake = parts$lake,
    schema = if (!is.null(schema_name)) schema_name else parts$schema,
    table = parts$table
  )
}

#' Render a table or view reference, fully qualified
#'
#' `lake.schema.path.table` from the parts `resolve_table_ref()` gives: the
#' lake named in `table_name`, else `ducklake_name`, else the current
#' database; `main` when no schema is named. Spelling the lake out keeps a
#' schema path from being read as an attached catalog of the same name,
#' which is how DuckDB resolves an unqualified first component.
#'
#' @param table_name A table or view name, optionally qualified.
#' @param conn A DBI connection.
#' @param ducklake_name The lake, when the caller has resolved it already.
#' @returns A quoted identifier path.
#' @noRd
table_sql <- function(table_name, conn = get_ducklake_connection(),
                      ducklake_name = NULL) {
  ref <- resolve_table_ref(table_name)
  lake <- ref$lake
  if (is.null(lake)) {
    lake <- infer_ducklake_name(ducklake_name, conn)
  }
  quote_ident(
    paste(
      c(lake, if (is.null(ref$schema)) "main" else ref$schema, ref$table),
      collapse = "."
    ),
    conn
  )
}

#' Render the target of a COMMENT ON statement
#'
#' DuckDB's `COMMENT ON` parser takes at most three qualifiers
#' (`catalog.schema.name`), so a table in a nested schema cannot be spelled
#' with its lake. A top-level schema is written `lake.schema.table`; one
#' level of nesting is written `schema.sub.table` and resolves in the
#' current database, which must be the lake; anything deeper has no
#' spelling at all, and `comments_reachable()` says so ahead of time.
#'
#' @inheritParams table_sql
#' @returns A quoted identifier path.
#' @noRd
comment_target_sql <- function(table_name, conn = get_ducklake_connection(),
                               ducklake_name = NULL) {
  ref <- resolve_table_ref(table_name)
  lake <- ref$lake
  if (is.null(lake)) {
    lake <- infer_ducklake_name(ducklake_name, conn)
  }
  schema <- if (is.null(ref$schema)) "main" else ref$schema
  depth <- schema_depth(schema)
  if (depth == 1) {
    return(quote_ident(paste(lake, schema, ref$table, sep = "."), conn))
  }
  if (depth > 2) {
    cli::cli_abort(c(
      "DuckDB cannot comment on {.val {table_name}}: {.code COMMENT ON} takes at most two schema levels.",
      "i" = "Schema {.val {schema}} is nested {depth - 1} levels deep. Comments and labels reach tables one level deep, such as {.val sales.europe.orders}."
    ))
  }
  current <- tryCatch(
    DBI::dbGetQuery(conn, "SELECT current_database() AS db")$db,
    error = function(e) NA_character_
  )
  if (!identical(current, lake)) {
    cli::cli_abort(c(
      "Commenting on {.val {table_name}} needs {.val {lake}} to be the current database.",
      "i" = "{.code COMMENT ON} cannot name the lake of a table in a nested schema. {.fun attach_ducklake} makes a lake current, as does {.code USE {lake}}."
    ))
  }
  quote_ident(paste(schema, ref$table, sep = "."), conn)
}

#' A table name with its schema spelled out, `main` when none was given
#'
#' For the readers that take an unqualified name as that table name in
#' every schema, when one table is meant. A leading lake name is dropped.
#' @noRd
qualify_table_name <- function(table_name) {
  ref <- resolve_table_ref(table_name)
  paste(c(if (is.null(ref$schema)) "main" else ref$schema, ref$table), collapse = ".")
}

#' Can comments (and so labels) be stored on this table at all?
#'
#' `FALSE` for a table nested two or more schema levels deep, which
#' DuckDB's `COMMENT ON` cannot name.
#' @noRd
comments_reachable <- function(table_name) {
  ref <- resolve_table_ref(table_name)
  is.null(ref$schema) || schema_depth(ref$schema) <= 2
}

#' Number of components in a dotted schema path
#' @noRd
schema_depth <- function(schema) {
  length(strsplit(schema, ".", fixed = TRUE)[[1]])
}

#' A recursive CTE giving every schema of a lake with its full path
#'
#' Over `duckdb_schemas()`, so it follows a snapshot-pinned attach and an
#' open transaction and works whatever the catalog's format version. Join
#' `schema_paths` on `schema_oid`; the path is `schema_path`. The lake name
#' is interpolated, having passed `check_identifier()`.
#'
#' @param ducklake_name The lake.
#' @returns The text of one CTE, to follow `WITH RECURSIVE`.
#' @noRd
catalog_schema_paths_cte <- function(ducklake_name) {
  sprintf(
    "schema_paths AS (
       SELECT oid, schema_name AS schema_path
       FROM duckdb_schemas()
       WHERE database_name = %1$s AND parent_schema_oid IS NULL
       UNION ALL
       SELECT c.oid, p.schema_path || '.' || c.schema_name
       FROM duckdb_schemas() c
       JOIN schema_paths p ON c.parent_schema_oid = p.oid
       WHERE c.database_name = %1$s
     )",
    quote_sql(ducklake_name)
  )
}

#' A CTE giving every schema row of a lake's metadata with its full path
#'
#' Over `ducklake_schema`, for the queries that read history. Each row
#' keeps its own `begin_snapshot` and `end_snapshot`, so the validity
#' filters callers put on `s` still apply. A 1.0 catalog has no
#' `parent_schema_id` column, and no nested schemas, so there the CTE is a
#' flat read.
#'
#' @param ducklake_name The lake.
#' @param conn A DBI connection.
#' @returns The text of one CTE, to follow `WITH RECURSIVE`.
#' @noRd
metadata_schema_paths_cte <- function(ducklake_name, conn = get_ducklake_connection()) {
  prefix <- metadata_prefix(ducklake_name, conn)
  if (lake_has_nested_schemas(ducklake_name, conn)) {
    sprintf(
      "schema_paths AS (
         SELECT schema_id, schema_name AS schema_path, begin_snapshot, end_snapshot
         FROM %1$s.ducklake_schema
         WHERE parent_schema_id IS NULL
         UNION ALL
         SELECT c.schema_id, p.schema_path || '.' || c.schema_name,
                c.begin_snapshot, c.end_snapshot
         FROM %1$s.ducklake_schema c
         JOIN schema_paths p ON c.parent_schema_id = p.schema_id
       )",
      prefix
    )
  } else {
    sprintf(
      "schema_paths AS (
         SELECT schema_id, schema_name AS schema_path, begin_snapshot, end_snapshot
         FROM %s.ducklake_schema
       )",
      prefix
    )
  }
}

#' Does the lake's catalog format have nested schemas (1.1 or later)?
#'
#' Read from `options()`, not probed with a statement that could fail,
#' because a failed statement aborts a caller's open transaction. Cached
#' in the lake's registry entry, which every attach renews.
#' @noRd
lake_has_nested_schemas <- function(ducklake_name, conn = get_ducklake_connection()) {
  entry <- .ducklake_env$lakes[[ducklake_name]]
  if (!is.null(entry$nested_schemas)) {
    return(entry$nested_schemas)
  }
  version <- tryCatch(
    DBI::dbGetQuery(
      conn,
      sprintf(
        "SELECT value FROM %s.options() WHERE option_name = 'version'",
        quote_ident(ducklake_name, conn)
      )
    )$value,
    error = function(e) character()
  )
  nested <- length(version) == 1 && !is.na(version) && version != "1.0"
  if (!is.null(entry)) {
    .ducklake_env$lakes[[ducklake_name]]$nested_schemas <- nested
  }
  nested
}

#' WHERE fragment and parameters that select one table in a metadata query
#'
#' The query must alias `ducklake_table` as `t` and the schema-path CTE as
#' `s`, or pass the columns it uses instead. With no schema in the name the
#' filter matches the table name in every schema; with one it matches the
#' full schema path.
#'
#' @param table_name A table name, optionally qualified, or `NULL` for no
#'   filter.
#' @param table_col The SQL column holding the object name.
#' @param schema_col The SQL column holding the schema path.
#' @returns A list with `sql` (starting with `AND`, or empty) and `params`.
#' @noRd
table_filter <- function(table_name, table_col = "t.table_name",
                         schema_col = "s.schema_path") {
  if (is.null(table_name)) {
    return(list(sql = "", params = list()))
  }
  ref <- resolve_table_ref(table_name)
  if (is.null(ref$schema)) {
    list(sql = sprintf("AND %s = ?", table_col), params = list(ref$table))
  } else {
    list(
      sql = sprintf("AND %s = ? AND %s = ?", table_col, schema_col),
      params = list(ref$table, ref$schema)
    )
  }
}

#' Emit a routine confirmation, unless the session turned them off
#'
#' Every operation confirms what it did through this wrapper, so a
#' pipeline can silence the chatter with `options(ducklake.verbose =
#' FALSE)`. Warnings, errors, and notices about extension downloads are
#' emitted directly and stay on. Messages carry the class
#' `ducklake_message` for handlers that want to route them.
#'
#' @param message,... Passed to [cli::cli_inform()].
#' @param .envir Environment for glue interpolation; the caller's frame.
#' @returns Invisibly, `NULL`.
#' @noRd
dl_inform <- function(message, ..., .envir = parent.frame()) {
  if (!isTRUE(getOption("ducklake.verbose", TRUE))) {
    return(invisible(NULL))
  }
  cli::cli_inform(message, ..., class = "ducklake_message", .envir = .envir)
  invisible(NULL)
}

#' Create a local directory if it is missing; leave remote URIs alone
#' @noRd
ensure_local_dir <- function(path) {
  if (!is_remote_path(path) && !dir.exists(path)) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }
  invisible(path)
}

#' Validate an optional free-text argument
#'
#' `NULL` passes; otherwise the value must be one non-empty string. An
#' empty string is refused so that an unset environment variable, read
#' with `Sys.getenv()` into an author or a message, fails here instead of
#' being recorded as `""`.
#'
#' @param x The value to validate.
#' @param arg Argument name for the error message.
#' @returns `x`, invisibly.
#' @noRd
check_optional_string <- function(x, arg) {
  if (!is.null(x) &&
      (!is.character(x) || length(x) != 1 || is.na(x) || !nzchar(x))) {
    cli::cli_abort("{.arg {arg}} must be `NULL` or a single non-empty string.")
  }
  invisible(x)
}

#' The author for a snapshot this session commits
#'
#' An explicit `author` wins; otherwise the `ducklake.author` option, when
#' set; otherwise `NULL`, which records no author. Used at the package's
#' commit points (`commit_transaction()`, and through it
#' `with_transaction()` and `restore_table_version()`) and for the creation
#' snapshot `attach_ducklake()` labels. Not used by
#' `set_snapshot_metadata()`: the person labeling a snapshot after the fact
#' is not always the one who committed it.
#'
#' @param author An author name, or `NULL` to fall back to the option.
#' @returns A single string, or `NULL`.
#' @noRd
resolve_author <- function(author = NULL) {
  check_optional_string(author, "author")
  if (!is.null(author)) {
    return(author)
  }
  option <- getOption("ducklake.author")
  if (!is.null(option) &&
      (!is.character(option) || length(option) != 1 || is.na(option) ||
         !nzchar(option))) {
    cli::cli_abort(
      "The {.code ducklake.author} option must be a single non-empty string, or `NULL`."
    )
  }
  option
}
