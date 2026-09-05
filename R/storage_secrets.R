#' Store object storage credentials for a session
#'
#' Registers credentials with DuckDB's secrets manager so a DuckLake can
#' read and write data files on object storage (`lake_path = "s3://..."`
#' and friends). Wraps `CREATE SECRET`.
#'
#' @param type Storage type: `"s3"` (also for S3-compatible stores),
#'   `"gcs"` (Google Cloud Storage), `"r2"` (Cloudflare R2), or `"azure"`.
#' @param ... Named secret parameters passed through to `CREATE SECRET`,
#'   e.g. `key_id`, `secret`, `region`, `session_token`, `endpoint`,
#'   `url_style`, `account_id` (R2), or `connection_string` (Azure).
#'   Character values are quoted; logicals become `true`/`false`.
#' @param provider Optional credential provider. The common one is
#'   `"credential_chain"`, which picks up credentials the way AWS SDKs do
#'   (environment variables, profiles, instance metadata) so no key needs to
#'   be passed in code. For `"s3"`, `"gcs"`, and `"r2"` secrets this provider
#'   lives in DuckDB's aws extension, which is loaded (and installed on first
#'   use) automatically.
#' @param scope Optional URI prefix (e.g. `"s3://my-bucket"`) limiting which
#'   paths the secret applies to. Useful when different buckets need
#'   different credentials.
#' @param name Optional name for the secret. Named secrets can be replaced
#'   and dropped individually; unnamed ones act as the default for their
#'   type.
#' @param persistent If `TRUE`, the secret is written (unencrypted) to
#'   DuckDB's secret directory and survives the session. Where that is
#'   depends on the duckdb R package: from 1.5.2 on it sits under the same
#'   "home" directory as extensions (`DUCKDB_R_HOME`, the `duckdb.home`
#'   option, or `~/.duckdb`; see [install_ducklake()]), and with the
#'   temporary default the secret is lost with the session anyway. The
#'   default `FALSE` keeps it in memory only, which is the right choice for
#'   credentials supplied from a vault or environment variable.
#'
#' @details
#' The httpfs extension (or the azure extension for `type = "azure"`) is
#' loaded automatically. With `provider = "credential_chain"`, the aws
#' extension is loaded too: it supplies that provider for `"s3"`, `"gcs"`,
#' and `"r2"` secrets, and DuckDB's automatic mid-statement install of it
#' can fail, so the package loads it up front instead. If you pre-install
#' extensions (say, when baking a container image), include `aws` alongside
#' `httpfs`. The azure extension provides its own credential chain.
#'
#' On Windows neither the aws nor the azure extension is available for the
#' duckdb R package, so `provider = "credential_chain"` and `type = "azure"`
#' fail there; explicit keys for `"s3"`, `"gcs"`, and `"r2"` work.
#'
#' Prefer `provider = "credential_chain"` over embedding long-lived keys in
#' scripts. The secret's values are visible in the session via
#' `duckdb_secrets()` (redacted) and travel with persistent storage
#' unencrypted, so treat `persistent = TRUE` with the same care as a
#' credentials file.
#'
#' @returns Invisibly returns the secret's name (or `NA_character_` for an
#'   unnamed secret).
#' @family connection management
#' @export
#'
#' @seealso [attach_ducklake()]
#'
#' @examples
#' \dontrun{
#' # Explicit keys, scoped to one bucket
#' create_storage_secret(
#'   "s3",
#'   key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
#'   secret = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
#'   region = "us-east-1",
#'   scope = "s3://my-trial-lake"
#' )
#'
#' # Let the AWS credential chain find credentials
#' create_storage_secret("s3", provider = "credential_chain")
#'
#' # Then attach a lake whose data lives on S3. The catalog file stays on
#' # local disk; lake_path only sets where the Parquet data goes.
#' attach_ducklake(
#'   "trial_lake",
#'   lake_path = "s3://my-trial-lake/data",
#'   catalog_connection_string = "trial_lake.ducklake"
#' )
#' }
create_storage_secret <- function(type = c("s3", "gcs", "r2", "azure"),
                                  ...,
                                  provider = NULL,
                                  scope = NULL,
                                  name = NULL,
                                  persistent = FALSE) {
  type <- match.arg(type)
  conn <- get_ducklake_connection()

  extension <- if (type == "azure") "azure" else "httpfs"
  load_or_install_extension(extension, conn = conn)

  # credential_chain lives in the aws extension for s3/gcs/r2 (azure ships its
  # own); DuckDB's mid-statement autoload of aws is unreliable, so load it here
  if (identical(provider, "credential_chain") && type != "azure") {
    load_or_install_extension("aws", conn = conn)
  }

  params <- list(...)
  if (length(params) > 0 &&
      (is.null(names(params)) || any(names(params) == ""))) {
    cli::cli_abort("All secret parameters in {.arg ...} must be named.")
  }
  bad <- names(params)[!grepl("^[A-Za-z_][A-Za-z0-9_]*$", names(params))]
  if (length(bad) > 0) {
    cli::cli_abort("Invalid secret parameter name{?s}: {.val {bad}}.")
  }

  if (!is.null(provider)) {
    check_identifier(provider, arg = "provider")
  }
  if (!is.null(name)) {
    check_identifier(name, arg = "name")
  }

  fields <- c(
    sprintf("TYPE %s", type),
    if (!is.null(provider)) sprintf("PROVIDER %s", provider),
    vapply(
      names(params),
      function(key) {
        sprintf("%s %s", toupper(key), render_secret_value(params[[key]]))
      },
      character(1)
    ),
    if (!is.null(scope)) sprintf("SCOPE %s", quote_sql(scope))
  )

  sql <- sprintf(
    "CREATE OR REPLACE %sSECRET %s(\n  %s\n);",
    if (persistent) "PERSISTENT " else "",
    if (is.null(name)) "" else paste0(name, " "),
    paste(fields, collapse = ",\n  ")
  )
  db_execute(sql, conn = conn)

  cli::cli_inform(
    "Created {if (persistent) 'persistent ' else ''}{.val {type}} storage secret{if (!is.null(name)) ' {.val {name}}' else ''}."
  )

  invisible(if (is.null(name)) NA_character_ else name)
}

#' Render a secret parameter value as a CREATE SECRET literal
#'
#' @noRd
render_secret_value <- function(value) {
  if (length(value) != 1 || is.na(value)) {
    cli::cli_abort("Secret parameter values must be single non-missing values.")
  }
  if (is.logical(value)) {
    if (value) "true" else "false"
  } else if (is.numeric(value)) {
    format(value, scientific = FALSE)
  } else {
    quote_sql(as.character(value))
  }
}
