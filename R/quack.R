# Quack remote protocol support
#
# Quack turns a DuckDB instance into a server that other DuckDB instances reach
# over a `quack:` URI. It shipped as a core extension in DuckDB 1.5.3. These
# helpers install the extension, connect to a remote Quack server, run one-off
# remote queries, and serve the current session (including any attached
# DuckLake) to other clients.

#' Install the Quack extension
#'
#' Installs the Quack DuckDB extension, which provides the `quack:` client-server
#' protocol. Quack is a core extension from DuckDB 1.5.3 onward, so it is
#' autoloaded the first time it is used. Installing it ahead of time is useful on
#' machines that have no internet access at query time.
#'
#' @param load If `TRUE` (the default), load the extension after installing it.
#'
#' @returns NULL
#' @export
#'
#' @seealso [attach_quack()], [quack_serve()]
#'
#' @examples
#' \dontrun{
#' install_quack()
#' }
install_quack <- function(load = TRUE) {
  check_quack_version()

  db_execute("INSTALL quack;")
  cli::cli_inform("Installed {.pkg quack} extension.")

  if (load) {
    db_execute("LOAD quack;")
    cli::cli_inform("Loaded {.pkg quack} extension.")
  }

  invisible(NULL)
}

#' Connect to a remote Quack server
#'
#' Attaches a remote Quack server as a catalog in the current session. Tables in
#' the server's default database are then reachable as `quack_name.table_name`
#' and can be queried with [get_ducklake_table()] or `dplyr::tbl()`.
#'
#' A DuckLake served over Quack lives in its own catalog on the server rather
#' than in the default database, so its tables are not exposed under
#' `quack_name`. Query a served DuckLake with [quack_query()], naming the lake's
#' catalog, for example `quack_query(uri, "SELECT * FROM trial.adsl")`.
#'
#' @param quack_name Name for the attached remote catalog, used as the database
#'   alias in DuckDB.
#' @param uri Address of the Quack server, for example `"quack:localhost"` or
#'   `"quack:data.example.org:9494"`. A bare host such as `"localhost"` is
#'   prefixed with `quack:` automatically. The default port is 9494.
#' @param token Authentication token expected by the server. If `NULL`, the token
#'   is taken from a Quack secret (see `CREATE SECRET`) if one exists.
#' @param disable_ssl Connect over plain HTTP instead of HTTPS (default `FALSE`).
#'   Only appropriate on a trusted network.
#'
#' @returns NULL
#' @export
#'
#' @seealso [detach_quack()], [quack_query()], [quack_serve()]
#'
#' @examples
#' \dontrun{
#' # A remote DuckDB database, queried through the attached catalog
#' attach_quack("warehouse", "quack:data.example.org", token = "super_secret")
#'
#' get_ducklake_table("warehouse.sales") |>
#'   dplyr::filter(region == "EMEA") |>
#'   dplyr::collect()
#'
#' detach_quack("warehouse")
#' }
attach_quack <- function(quack_name, uri, token = NULL, disable_ssl = FALSE) {
  if (missing(uri) || is.null(uri)) {
    cli::cli_abort(c(
      "A {.arg uri} is required.",
      "i" = "This is the address of the Quack server, for example {.val quack:localhost}."
    ))
  }

  uri <- build_quack_uri(uri)
  check_quack_version()
  ensure_quack_extension()

  conn <- get_ducklake_connection()

  # If this name is already attached, leave it in place
  attached <- tryCatch(
    DBI::dbGetQuery(conn, "SELECT database_name FROM duckdb_databases();")$database_name,
    error = function(e) character(0)
  )

  if (quack_name %in% attached) {
    return(invisible(NULL))
  }

  attach_sql <- build_quack_attach_sql(quack_name, uri, token, disable_ssl)
  db_execute(attach_sql)

  invisible(NULL)
}

#' Disconnect from a remote Quack server
#'
#' Detaches a remote catalog previously attached with [attach_quack()]. The
#' DuckDB connection itself stays alive.
#'
#' @param quack_name Name of the remote catalog to detach. If `NULL`, nothing is
#'   detached.
#'
#' @returns NULL
#' @export
#'
#' @seealso [attach_quack()]
#'
#' @examples
#' \dontrun{
#' detach_quack("team")
#' }
detach_quack <- function(quack_name = NULL) {
  conn <- get_ducklake_connection()

  is_valid <- tryCatch(DBI::dbIsValid(conn), error = function(e) FALSE)
  if (!is_valid) {
    return(invisible(NULL))
  }

  if (!is.null(quack_name)) {
    # Switch off the remote catalog first: DuckDB cannot DETACH the
    # database currently in use
    use_home_database(conn)
    tryCatch(
      DBI::dbExecute(conn, sprintf("DETACH %s;", quote_ident(quack_name, conn))),
      error = function(e) NULL
    )
  }

  invisible(NULL)
}

#' Run a one-off query against a remote Quack server
#'
#' Sends a single SQL query to a Quack server and returns the result as a
#' data.frame. Unlike [attach_quack()], this does not attach the remote catalog,
#' so it is a quick way to pull a result without changing the session state.
#'
#' @param uri Address of the Quack server, for example `"quack:localhost"`.
#' @param query A SQL query string to run on the server.
#' @param token Authentication token expected by the server. If `NULL`, a Quack
#'   secret is used if one exists.
#' @param disable_ssl Connect over plain HTTP instead of HTTPS (default `FALSE`).
#'
#' @returns A data.frame with the query result.
#' @export
#'
#' @seealso [attach_quack()]
#'
#' @examples
#' \dontrun{
#' quack_query(
#'   "quack:data.example.org",
#'   "SELECT count(*) FROM adsl",
#'   token = "super_secret"
#' )
#' }
quack_query <- function(uri, query, token = NULL, disable_ssl = FALSE) {
  if (missing(query) || !is.character(query) || length(query) != 1 || is.na(query)) {
    cli::cli_abort("{.arg query} must be a single SQL string.")
  }

  uri <- build_quack_uri(uri)
  check_quack_version()
  ensure_quack_extension()

  args <- c(quote_sql(uri), quote_sql(query))
  if (!is.null(token)) {
    args <- c(args, sprintf("token = %s", quote_sql(token)))
  }
  if (disable_ssl) {
    args <- c(args, "disable_ssl = true")
  }

  sql <- sprintf("SELECT * FROM quack_query(%s);", paste(args, collapse = ", "))

  conn <- get_ducklake_connection()
  DBI::dbGetQuery(conn, sql)
}

#' Serve the current session over Quack
#'
#' Starts a Quack server in the current DuckDB instance. Everything attached to
#' the session, including a DuckLake attached with [attach_ducklake()], becomes
#' reachable by other DuckDB clients over the `quack:` protocol. The server runs
#' in the background of the DuckDB instance, so the R session stays usable.
#'
#' @param uri Address to listen on (default `"quack:localhost"`). The default
#'   port is 9494.
#' @param token Authentication token that clients must supply. If `NULL`, the
#'   server accepts any client that can reach it, and a warning is issued.
#' @param allow_other_hostname Accept connections addressed to a hostname other
#'   than the one in `uri` (default `FALSE`).
#' @param disable_ssl Serve over plain HTTP instead of HTTPS (default `FALSE`).
#'   Only appropriate on a trusted network.
#'
#' @returns The server `uri`, invisibly.
#' @export
#'
#' @seealso [quack_stop()], [attach_quack()]
#'
#' @examples
#' \dontrun{
#' attach_ducklake("trial", lake_path = "~/lakes/trial")
#' quack_serve(token = "super_secret")
#' # ... colleagues connect with attach_quack() ...
#' quack_stop()
#' }
quack_serve <- function(uri = "quack:localhost", token = NULL,
                        allow_other_hostname = FALSE, disable_ssl = FALSE) {
  uri <- build_quack_uri(uri)
  check_quack_version()
  ensure_quack_extension()

  if (is.null(token)) {
    cli::cli_warn(c(
      "Starting a Quack server without a {.arg token}.",
      "!" = "Anyone who can reach {.val {uri}} can read and write the served data.",
      "i" = "Pass {.arg token} to require authentication."
    ))
  }

  args <- quote_sql(uri)
  if (!is.null(token)) {
    args <- c(args, sprintf("token = %s", quote_sql(token)))
  }
  if (allow_other_hostname) {
    args <- c(args, "allow_other_hostname = true")
  }
  if (disable_ssl) {
    args <- c(args, "disable_ssl = true")
  }

  db_execute(sprintf("CALL quack_serve(%s);", paste(args, collapse = ", ")))
  cli::cli_inform("Quack server listening on {.val {uri}}.")

  invisible(uri)
}

#' Stop a Quack server
#'
#' Stops a Quack server started with [quack_serve()].
#'
#' @param uri Address the server is listening on (default `"quack:localhost"`).
#'
#' @returns `TRUE`, invisibly.
#' @export
#'
#' @seealso [quack_serve()]
#'
#' @examples
#' \dontrun{
#' quack_stop()
#' }
quack_stop <- function(uri = "quack:localhost") {
  uri <- build_quack_uri(uri)
  check_quack_version()
  ensure_quack_extension()

  db_execute(sprintf("CALL quack_stop(%s);", quote_sql(uri)))
  cli::cli_inform("Quack server on {.val {uri}} stopped.")

  invisible(TRUE)
}

#' Check that the active DuckDB engine supports Quack
#'
#' @param conn Optional DuckDB connection. Defaults to the ducklake connection.
#' @keywords internal
check_quack_version <- function(conn = NULL) {
  if (is.null(conn)) {
    conn <- get_ducklake_connection()
  }

  version <- DBI::dbGetQuery(conn, "SELECT version() AS version")$version

  if (!quack_version_supported(version)) {
    cli::cli_abort(c(
      "Quack requires DuckDB 1.5.3 or higher.",
      "i" = "The active DuckDB engine reports version {version}.",
      "i" = "Quack shipped as a core extension in DuckDB 1.5.3. Update the {.pkg duckdb} R package to a build of 1.5.3 or newer."
    ))
  }

  invisible(TRUE)
}

#' Test a DuckDB version string against the Quack minimum
#'
#' @param version A DuckDB version string such as `"v1.5.3"`.
#' @returns `TRUE` if `version` is 1.5.3 or higher, otherwise `FALSE`.
#' @keywords internal
quack_version_supported <- function(version) {
  cleaned <- sub("^v", "", version)
  core <- regmatches(cleaned, regexpr("^[0-9]+\\.[0-9]+\\.[0-9]+", cleaned))
  if (length(core) == 0) {
    return(FALSE)
  }
  numeric_version(core) >= "1.5.3"
}

#' Load the Quack extension, installing it first if needed
#'
#' @keywords internal
ensure_quack_extension <- function() {
  tryCatch(
    db_execute("LOAD quack;"),
    error = function(e) {
      db_execute("INSTALL quack;")
      db_execute("LOAD quack;")
    }
  )
  invisible(NULL)
}

#' Normalize a Quack URI
#'
#' Validates that `uri` is a single non-empty string and prepends the `quack:`
#' scheme if it is missing.
#'
#' @param uri A Quack server address.
#' @returns The normalized URI string.
#' @keywords internal
build_quack_uri <- function(uri) {
  if (!is.character(uri) || length(uri) != 1 || is.na(uri) || !nzchar(uri)) {
    cli::cli_abort(c(
      "{.arg uri} must be a single, non-empty string.",
      "i" = "For example {.val quack:localhost} or {.val quack:data.example.org:9494}."
    ))
  }

  if (!grepl("^quack:", uri)) {
    uri <- paste0("quack:", uri)
  }

  uri
}

#' Build the ATTACH SQL for a Quack server
#'
#' @param quack_name Name for the remote catalog alias.
#' @param uri Quack server address.
#' @param token Optional authentication token.
#' @param disable_ssl Whether to add the `DISABLE_SSL` option.
#'
#' @returns A SQL ATTACH statement string.
#' @keywords internal
build_quack_attach_sql <- function(quack_name, uri, token = NULL, disable_ssl = FALSE) {
  check_identifier(quack_name, arg = "quack_name")
  uri <- build_quack_uri(uri)

  options <- "TYPE quack"
  if (!is.null(token)) {
    options <- c(options, sprintf("TOKEN %s", quote_sql(token)))
  }
  if (disable_ssl) {
    options <- c(options, "DISABLE_SSL")
  }

  options_str <- paste(options, collapse = ", ")
  sprintf("ATTACH %s AS %s (%s);", quote_sql(uri), quack_name, options_str)
}

