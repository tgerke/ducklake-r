#' Configure how DuckLake retries conflicting transactions
#'
#' With several writers on one lake (a PostgreSQL or SQLite catalog, or a
#' Quack server), two transactions can try to commit the next snapshot at
#' the same time. DuckLake settles the race itself: the transaction that
#' lost checks whether its changes conflict with the one that won, and if
#' they do not it is retried against the new snapshot. Only real conflicts
#' reach you as an error: two transactions deleting from the same data
#' file, an insert into a table the other transaction dropped or altered,
#' or two creates of the same name. This function sets the DuckDB settings
#' that control the retries. DuckLake's defaults are 10 attempts, 100 ms
#' apart, with the wait growing by a factor of 1.5 each time.
#'
#' The settings belong to the connection, not the lake, so set them in each
#' session that writes concurrently. Called with no arguments, the function
#' only reports the current values.
#'
#' @param max_retries Maximum number of retry attempts (a non-negative
#'   integer).
#' @param wait_ms Milliseconds to wait before the first retry.
#' @param backoff Factor by which the wait grows after each retry (1 keeps
#'   it constant).
#'
#' @returns Invisibly, a data frame with the current `name` and `value` of
#'   the three settings.
#' @family transactions
#' @export
#'
#' @examplesIf ducklake_extension_available()
#' # Be more patient with a busy shared catalog
#' set_ducklake_retry(max_retries = 20, wait_ms = 250, backoff = 2)
#'
#' # Read the current values
#' set_ducklake_retry()
set_ducklake_retry <- function(max_retries = NULL, wait_ms = NULL, backoff = NULL) {
  conn <- get_ducklake_connection()

  check_count <- function(x, arg) {
    if (!is.numeric(x) || length(x) != 1 || is.na(x) || x < 0 || x != round(x)) {
      cli::cli_abort("{.arg {arg}} must be a single non-negative integer.")
    }
    as.integer(x)
  }
  if (!is.null(max_retries)) {
    db_execute(
      sprintf("SET ducklake_max_retry_count = %d;", check_count(max_retries, "max_retries")),
      conn = conn
    )
  }
  if (!is.null(wait_ms)) {
    db_execute(
      sprintf("SET ducklake_retry_wait_ms = %d;", check_count(wait_ms, "wait_ms")),
      conn = conn
    )
  }
  if (!is.null(backoff)) {
    if (!is.numeric(backoff) || length(backoff) != 1 || is.na(backoff) || backoff < 1) {
      cli::cli_abort("{.arg backoff} must be a single number of at least 1.")
    }
    db_execute(
      sprintf("SET ducklake_retry_backoff = %s;", format(backoff, scientific = FALSE)),
      conn = conn
    )
  }

  settings <- DBI::dbGetQuery(
    conn,
    "SELECT name, value FROM duckdb_settings()
     WHERE name IN ('ducklake_max_retry_count', 'ducklake_retry_wait_ms', 'ducklake_retry_backoff')
     ORDER BY name"
  )
  value_of <- function(name) settings$value[settings$name == name]
  cli::cli_inform(
    "DuckLake retries a conflicting transaction up to {value_of('ducklake_max_retry_count')} times, starting {value_of('ducklake_retry_wait_ms')} ms apart with backoff {value_of('ducklake_retry_backoff')}."
  )

  invisible(settings)
}
