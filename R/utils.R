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
#' Says so before installing: a download into DuckDB's extension directory
#' should never be a silent side effect of calling an unrelated function.
#' The message names the directory and warns when it is a temporary one.
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

#' Test a DuckDB engine version string against a minimum
#'
#' Tolerates a leading `v` and dev/suffixed versions such as
#' `"v1.5.4-dev123"` by comparing only the leading `major.minor.patch`.
#'
#' @param version Version string as reported by `SELECT version()`.
#' @param minimum Minimum required version, e.g. `"1.5.1"`.
#' @returns `TRUE` if `version` is at least `minimum`, otherwise `FALSE`
#'   (including when `version` cannot be parsed).
#' @noRd
duckdb_version_at_least <- function(version, minimum) {
  cleaned <- sub("^v", "", version)
  core <- regmatches(cleaned, regexpr("^[0-9]+\\.[0-9]+\\.[0-9]+", cleaned))
  if (length(core) == 0) {
    return(FALSE)
  }
  numeric_version(core) >= minimum
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
    # %OS6 keeps sub-second precision: flooring to the second can move the
    # instant before a snapshot taken in the same second
    format(x, "%Y-%m-%d %H:%M:%OS6", tz = "UTC")
  } else {
    as.character(x)
  }
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
  ids <- tryCatch(
    c(
      DBI::dbGetQuery(conn, "SELECT current_transaction_id() AS id")$id,
      DBI::dbGetQuery(conn, "SELECT current_transaction_id() AS id")$id
    ),
    error = function(e) NULL
  )
  length(ids) == 2 && ids[1] == ids[2]
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

#' Split a possibly schema-qualified table name
#'
#' `"cars"` gives `list(schema = NULL, table = "cars")`; `"bronze.cars"`
#' gives `list(schema = "bronze", table = "cars")`; a three-part name keeps
#' its last two parts. Nothing is quoted here.
#'
#' @param table_name A table name, optionally qualified with `.`.
#' @returns A list with `schema` (`NULL` when absent) and `table`.
#' @noRd
split_table_name <- function(table_name) {
  parts <- strsplit(table_name, ".", fixed = TRUE)[[1]]
  n <- length(parts)
  if (n <= 1) {
    return(list(schema = NULL, table = table_name))
  }
  list(schema = parts[[n - 1]], table = parts[[n]])
}

#' Quoted prefix of a lake's metadata catalog for SQL text
#'
#' DuckDB and SQLite catalogs keep the DuckLake tables in a `main` schema;
#' PostgreSQL and MySQL catalogs expose them at the top level. Resolves the
#' backend from the lake's own registry entry, not the current database.
#'
#' @param ducklake_name The lake name.
#' @param conn A DBI connection used for quoting rules.
#' @returns A string such as `"__ducklake_metadata_lake".main`.
#' @noRd
metadata_prefix <- function(ducklake_name, conn = get_ducklake_connection()) {
  meta_db <- paste0("__ducklake_metadata_", ducklake_name)
  if (get_ducklake_backend(ducklake_name) %in% c("postgres", "mysql")) {
    quote_ident(meta_db, conn)
  } else {
    paste0(quote_ident(meta_db, conn), ".main")
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
#' duckdb 1.5.2 and later resolve a "home" directory for extensions that
#' defaults to a per-session temporary directory unless `DUCKDB_R_HOME`
#' (or `options(duckdb.home = )`, or an existing `~/.duckdb`) points at
#' somewhere durable. An install into a temporary directory is repeated on
#' every session, so say so.
#'
#' @param dir The extension directory, as reported by DuckDB.
#' @returns A named character vector of bullets, empty when the directory
#'   is durable or unknown.
#' @noRd
extension_persistence_hint <- function(dir) {
  if (length(dir) != 1 || is.na(dir) || !nzchar(dir)) {
    return(character())
  }
  is_temp <- startsWith(
    normalizePath(dir, mustWork = FALSE),
    normalizePath(tempdir(), mustWork = FALSE)
  )
  if (!is_temp) {
    return(character())
  }
  c(
    "!" = "This directory is temporary: the extension is gone when the R session ends.",
    "i" = "To keep extensions between sessions, set {.envvar DUCKDB_R_HOME} to a durable directory (for example in {.file ~/.Renviron}) before duckdb is loaded."
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
    schema = if (!is.null(schema_name)) schema_name else parts$schema,
    table = parts$table
  )
}

#' WHERE fragment and parameters that select one table in a metadata query
#'
#' The query must alias `ducklake_table` as `t` (or pass another
#' `table_col`) and `ducklake_schema` as `s`. With no schema in the name
#' the filter matches the table name in every schema.
#'
#' @param table_name A table name, optionally qualified, or `NULL` for no
#'   filter.
#' @param table_col The SQL column holding the object name.
#' @returns A list with `sql` (starting with `AND`, or empty) and `params`.
#' @noRd
table_filter <- function(table_name, table_col = "t.table_name") {
  if (is.null(table_name)) {
    return(list(sql = "", params = list()))
  }
  ref <- resolve_table_ref(table_name)
  if (is.null(ref$schema)) {
    list(sql = sprintf("AND %s = ?", table_col), params = list(ref$table))
  } else {
    list(
      sql = sprintf("AND %s = ? AND s.schema_name = ?", table_col),
      params = list(ref$table, ref$schema)
    )
  }
}
