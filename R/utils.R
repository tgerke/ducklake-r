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
