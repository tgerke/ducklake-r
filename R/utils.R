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

#' Split a SQL select list on top-level commas
#'
#' Commas inside parentheses (function arguments, CASE expressions) and
#' inside quoted strings or identifiers are not split points.
#'
#' @param x A length-one character vector (the select list of a query).
#' @returns A character vector of expressions.
#' @keywords internal
#' @noRd
split_top_level_commas <- function(x) {
  chars <- strsplit(x, "", fixed = TRUE)[[1]]
  depth <- 0L
  quote_char <- ""
  parts <- character(0)
  current <- character(0)

  for (ch in chars) {
    if (nzchar(quote_char)) {
      current <- c(current, ch)
      if (ch == quote_char) quote_char <- ""
    } else if (ch %in% c("'", '"')) {
      quote_char <- ch
      current <- c(current, ch)
    } else if (ch == "(") {
      depth <- depth + 1L
      current <- c(current, ch)
    } else if (ch == ")") {
      depth <- depth - 1L
      current <- c(current, ch)
    } else if (ch == "," && depth == 0L) {
      parts <- c(parts, paste(current, collapse = ""))
      current <- character(0)
    } else {
      current <- c(current, ch)
    }
  }

  c(parts, paste(current, collapse = ""))
}
