# Quack remote protocol support
#
# Quack turns a DuckDB instance into a server that other DuckDB instances reach
# over a `quack:` URI. It is a core extension, stable since DuckDB 2.0. These
# helpers install the extension, connect to a remote Quack server, run one-off
# remote queries, and serve the current session (including any attached
# DuckLake) to other clients. DuckDB 2.0's CONNECT statement, which routes a
# whole session to a server, is not wrapped: on the duckdb R client it leaves
# the connection unable to prepare any further statement (duckdb/duckdb#27269).

#' Install the Quack extension
#'
#' Installs the Quack DuckDB extension, which provides the `quack:` client-server
#' protocol. Quack is a core extension, stable since DuckDB 2.0, so it is
#' autoloaded the first time it is used. Installing it ahead of time is useful on
#' machines that have no internet access at query time.
#'
#' @param load If `TRUE` (the default), load the extension after installing it.
#'
#' @returns Invisibly, `NULL`. Called for its side effect of installing the
#'   `quack` DuckDB extension into the local extension cache.
#' @family quack
#' @export
#'
#' @seealso [attach_quack()], [quack_serve()]
#'
#' @examples
#' \dontrun{
#' install_quack()
#' }
install_quack <- function(load = TRUE) {

  db_execute("INSTALL quack;")
  dl_inform("Installed {.pkg quack} extension.")

  if (load) {
    db_execute("LOAD quack;")
    dl_inform("Loaded {.pkg quack} extension.")
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
#' catalog, for example `quack_query(uri, "SELECT * FROM trial.adsl")`. To
#' work with a lake whose catalog a Quack server holds open, attach it with
#' `attach_ducklake(backend = "quack")` instead.
#'
#' DuckDB 2.0's `CONNECT` statement, which points a whole session at a
#' server, is not used here and should not be run on the package's
#' connection: on the duckdb R client it leaves the connection unable to
#' prepare any further statement
#' (<https://github.com/duckdb/duckdb/issues/27269>).
#'
#' @param quack_name Name for the attached remote catalog, used as the database
#'   alias in DuckDB.
#' @param uri Address of the Quack server, for example `"quack:localhost"` or
#'   `"quack:data.example.org:9494"`. A bare host such as `"localhost"` is
#'   prefixed with `quack:` automatically. The default port is 9494.
#' @param token Authentication token expected by the server. If `NULL`, the token
#'   is taken from a Quack secret (see `CREATE SECRET`) if one exists.
#' @param disable_ssl Connect over plain HTTP instead of HTTPS (default `FALSE`,
#'   which leaves the choice to DuckDB: plain HTTP for a local server, HTTPS
#'   otherwise). Only appropriate on a trusted network.
#' @param ssl_fingerprint Optional SHA-256 fingerprint (64 hex digits) of the
#'   server's certificate, for a server with a self-signed certificate; the
#'   value [quack_generate_keys()] returns on the server side.
#' @param client_id Optional identifier the server records for this client
#'   (at least 4 characters), shown by its `quack_active_connections()`.
#' @param heartbeat_timeout Optional heartbeat lease, in seconds, requested
#'   from the server.
#'
#' @returns Invisibly, `NULL`. Called for its side effect of attaching the
#'   remote catalog to the package's DuckDB connection.
#' @family quack
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
attach_quack <- function(quack_name, uri, token = NULL, disable_ssl = FALSE,
                         ssl_fingerprint = NULL, client_id = NULL,
                         heartbeat_timeout = NULL) {
  if (missing(uri) || is.null(uri)) {
    cli::cli_abort(c(
      "A {.arg uri} is required.",
      "i" = "This is the address of the Quack server, for example {.val quack:localhost}."
    ))
  }

  uri <- build_quack_uri(uri)
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

  attach_sql <- build_quack_attach_sql(
    quack_name, uri, token, disable_ssl,
    ssl_fingerprint = ssl_fingerprint, client_id = client_id,
    heartbeat_timeout = heartbeat_timeout
  )
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
#' @returns Invisibly, `NULL`. Called for its side effect of detaching the
#'   remote catalog.
#' @family quack
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
#' @param disable_ssl Connect over plain HTTP instead of HTTPS (default `FALSE`,
#'   which leaves the choice to DuckDB: plain HTTP for a local server, HTTPS
#'   otherwise).
#' @inheritParams attach_quack
#'
#' @returns A data.frame with the query result.
#' @family quack
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
quack_query <- function(uri, query, token = NULL, disable_ssl = FALSE,
                        ssl_fingerprint = NULL, client_id = NULL,
                        heartbeat_timeout = NULL) {
  if (missing(query) || !is.character(query) || length(query) != 1 || is.na(query)) {
    cli::cli_abort("{.arg query} must be a single SQL string.")
  }

  ensure_quack_extension()
  sql <- build_quack_query_sql(
    uri, query, token, disable_ssl,
    ssl_fingerprint = ssl_fingerprint, client_id = client_id,
    heartbeat_timeout = heartbeat_timeout
  )

  conn <- get_ducklake_connection()
  DBI::dbGetQuery(conn, sql)
}

#' Build the quack_query() SELECT for a one-off remote query
#'
#' @inheritParams quack_query
#' @returns A SELECT statement string.
#' @noRd
build_quack_query_sql <- function(uri, query, token = NULL, disable_ssl = FALSE,
                                  ssl_fingerprint = NULL, client_id = NULL,
                                  heartbeat_timeout = NULL) {
  uri <- build_quack_uri(uri)
  args <- c(quote_sql(uri), quote_sql(query))
  if (!is.null(token)) {
    args <- c(args, sprintf("token = %s", quote_sql(token)))
  }
  if (disable_ssl) {
    args <- c(args, "disable_ssl = true")
  }
  args <- c(args, quack_client_args(ssl_fingerprint, client_id, heartbeat_timeout, "="))
  sprintf("SELECT * FROM quack_query(%s);", paste(args, collapse = ", "))
}

#' Render the client-side connection options Quack takes
#'
#' The same three options go into `ATTACH` (as `NAME value`) and into
#' `quack_query()` (as `name = value`).
#'
#' @param sep `"="` for a function argument, `" "` for an ATTACH option.
#' @returns A character vector of rendered options, empty when none is set.
#' @noRd
quack_client_args <- function(ssl_fingerprint, client_id, heartbeat_timeout, sep) {
  render <- function(name, value) {
    if (sep == "=") {
      sprintf("%s = %s", tolower(name), value)
    } else {
      sprintf("%s %s", toupper(name), value)
    }
  }
  args <- character()
  if (!is.null(ssl_fingerprint)) {
    check_optional_string(ssl_fingerprint, "ssl_fingerprint")
    args <- c(args, render("ssl_fingerprint", quote_sql(ssl_fingerprint)))
  }
  if (!is.null(client_id)) {
    check_optional_string(client_id, "client_id")
    args <- c(args, render("client_id", quote_sql(client_id)))
  }
  if (!is.null(heartbeat_timeout)) {
    if (!is.numeric(heartbeat_timeout) || length(heartbeat_timeout) != 1 ||
        is.na(heartbeat_timeout) || heartbeat_timeout <= 0) {
      cli::cli_abort("{.arg heartbeat_timeout} must be a positive number of seconds.")
    }
    args <- c(args, render("heartbeat_timeout", sprintf("%d", as.integer(heartbeat_timeout))))
  }
  args
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
#' @param token Authentication token that clients must supply (at least 4
#'   characters). If `NULL`, the server takes the token of the session's
#'   default Quack secret when there is one (see `secret` and
#'   `create_secret_if_not_exists`), and otherwise generates a random token,
#'   which the returned data frame carries in `auth_token`. A server is never
#'   open: clients always present a token.
#' @param allow_other_hostname Accept connections addressed to a hostname other
#'   than the one in `uri` (default `FALSE`). A server reachable from other
#'   machines belongs behind a TLS-terminating reverse proxy.
#' @param disable_ssl Serve over plain HTTP instead of HTTPS (default `FALSE`,
#'   which leaves the choice to DuckDB: plain HTTP for a local address, HTTPS
#'   otherwise). Only appropriate on a trusted network.
#' @param secret Optional name of an existing Quack secret whose token the
#'   server uses.
#' @param create_secret_if_not_exists When no `token` is given and no default
#'   Quack secret exists, store the generated token in a persistent default
#'   secret, so later servers (and clients in this session) reuse it (default
#'   `FALSE`).
#' @param ssl_cert_file,ssl_key_file Optional certificate and private key
#'   files for serving over HTTPS, such as the pair [quack_generate_keys()]
#'   writes. Without them DuckDB generates a self-signed certificate when it
#'   serves HTTPS.
#'
#' @returns Invisibly, a data frame with one row: `listen_uri`, `listen_url`,
#'   and `auth_token`, the token clients must present.
#' @family quack
#' @export
#'
#' @seealso [quack_stop()], [attach_quack()], [quack_generate_keys()]
#'
#' @examples
#' \dontrun{
#' attach_ducklake("trial", lake_path = "path/to/lake")
#' quack_serve(token = "super_secret")
#' # ... colleagues connect with attach_quack() ...
#' quack_stop()
#'
#' # Let DuckDB pick the token and keep it in a secret for later servers
#' server <- quack_serve(create_secret_if_not_exists = TRUE)
#' server$auth_token
#' }
quack_serve <- function(uri = "quack:localhost", token = NULL,
                        allow_other_hostname = FALSE, disable_ssl = FALSE,
                        secret = NULL, create_secret_if_not_exists = FALSE,
                        ssl_cert_file = NULL, ssl_key_file = NULL) {
  uri <- build_quack_uri(uri)
  ensure_quack_extension()

  sql <- build_quack_serve_sql(
    uri, token, allow_other_hostname, disable_ssl,
    secret = secret, create_secret_if_not_exists = create_secret_if_not_exists,
    ssl_cert_file = ssl_cert_file, ssl_key_file = ssl_key_file
  )
  conn <- get_ducklake_connection()
  result <- DBI::dbGetQuery(conn, sql)
  if (is.null(token) && is.null(secret)) {
    dl_inform(c(
      "Quack server listening on {.val {uri}}.",
      "i" = "DuckDB chose the token; it is the {.field auth_token} of the returned data frame."
    ))
  } else {
    dl_inform("Quack server listening on {.val {uri}}.")
  }

  invisible(result)
}

#' Build the CALL quack_serve(...) statement
#'
#' @inheritParams quack_serve
#' @returns A CALL statement string.
#' @noRd
build_quack_serve_sql <- function(uri, token = NULL, allow_other_hostname = FALSE,
                                  disable_ssl = FALSE, secret = NULL,
                                  create_secret_if_not_exists = FALSE,
                                  ssl_cert_file = NULL, ssl_key_file = NULL) {
  uri <- build_quack_uri(uri)
  check_optional_string(token, "token")
  check_optional_string(secret, "secret")
  check_optional_string(ssl_cert_file, "ssl_cert_file")
  check_optional_string(ssl_key_file, "ssl_key_file")
  if (!is.null(token) && !is.null(secret)) {
    cli::cli_abort("Give {.arg token} or {.arg secret}, not both.")
  }
  if (is.null(ssl_cert_file) != is.null(ssl_key_file)) {
    cli::cli_abort("{.arg ssl_cert_file} and {.arg ssl_key_file} go together.")
  }

  args <- quote_sql(uri)
  if (!is.null(token)) {
    args <- c(args, sprintf("token = %s", quote_sql(token)))
  }
  if (!is.null(secret)) {
    args <- c(args, sprintf("secret = %s", quote_sql(secret)))
  }
  if (isTRUE(create_secret_if_not_exists)) {
    args <- c(args, "create_secret_if_not_exists = true")
  }
  if (allow_other_hostname) {
    args <- c(args, "allow_other_hostname = true")
  }
  if (disable_ssl) {
    args <- c(args, "disable_ssl = true")
  }
  if (!is.null(ssl_cert_file)) {
    args <- c(
      args,
      sprintf("ssl_cert_file = %s", quote_sql(ssl_cert_file)),
      sprintf("ssl_key_file = %s", quote_sql(ssl_key_file))
    )
  }
  sprintf("CALL quack_serve(%s);", paste(args, collapse = ", "))
}

#' Generate a self-signed certificate for a Quack server
#'
#' Writes `server.pem` and `private_key.pem` for [quack_serve()] to serve
#' HTTPS with, and returns their paths with the certificate's SHA-256
#' fingerprint, which clients pass as `ssl_fingerprint` to [attach_quack()]
#' or [quack_query()] so they can trust the self-signed certificate.
#' Existing key files are not overwritten.
#'
#' @param directory Directory to write the files into. `NULL` (the default)
#'   uses DuckDB's own location, `~/.duckdb/extension_data/quack`.
#'
#' @returns A data frame with one row: `cert_file`, `key_file`,
#'   `fingerprint`, and `status`.
#' @family quack
#' @export
#'
#' @seealso [quack_serve()]
#'
#' @examples
#' \dontrun{
#' keys <- quack_generate_keys(directory = "~/quack-keys")
#' quack_serve(
#'   "quack:0.0.0.0:9494", token = "super_secret",
#'   allow_other_hostname = TRUE,
#'   ssl_cert_file = keys$cert_file, ssl_key_file = keys$key_file
#' )
#' # Clients pin the certificate:
#' # attach_quack("team", "quack:host:9494", token = "super_secret",
#' #              ssl_fingerprint = keys$fingerprint)
#' }
quack_generate_keys <- function(directory = NULL) {
  check_optional_string(directory, "directory")
  ensure_quack_extension()
  args <- if (is.null(directory)) {
    ""
  } else {
    sprintf("directory = %s", quote_sql(normalize_lake_path(directory)))
  }
  conn <- get_ducklake_connection()
  DBI::dbGetQuery(conn, sprintf("CALL quack_generate_keys(%s);", args))
}

#' Stop a Quack server
#'
#' Stops a Quack server started with [quack_serve()].
#'
#' @param uri Address the server is listening on (default `"quack:localhost"`).
#'
#' @returns `TRUE`, invisibly.
#' @family quack
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
  ensure_quack_extension()

  db_execute(sprintf("CALL quack_stop(%s);", quote_sql(uri)))
  dl_inform("Quack server on {.val {uri}} stopped.")

  invisible(TRUE)
}

#' Load the Quack extension, installing it first if needed
#'
#' @returns Invisibly, `NULL`. Called for its side effect of loading the
#'   extension.
#' @keywords internal
ensure_quack_extension <- function() {
  load_or_install_extension("quack")
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
#' @param ssl_fingerprint,client_id,heartbeat_timeout Optional client
#'   options, rendered as `SSL_FINGERPRINT`, `CLIENT_ID`, and
#'   `HEARTBEAT_TIMEOUT`.
#'
#' @returns A SQL ATTACH statement string.
#' @keywords internal
build_quack_attach_sql <- function(quack_name, uri, token = NULL, disable_ssl = FALSE,
                                   ssl_fingerprint = NULL, client_id = NULL,
                                   heartbeat_timeout = NULL) {
  check_identifier(quack_name, arg = "quack_name")
  uri <- build_quack_uri(uri)

  options <- "TYPE quack"
  if (!is.null(token)) {
    options <- c(options, sprintf("TOKEN %s", quote_sql(token)))
  }
  if (disable_ssl) {
    options <- c(options, "DISABLE_SSL")
  }
  options <- c(
    options,
    quack_client_args(ssl_fingerprint, client_id, heartbeat_timeout, " ")
  )

  options_str <- paste(options, collapse = ", ")
  sprintf("ATTACH %s AS %s (%s);", quote_sql(uri), quack_name, options_str)
}

