#' Create or attach a ducklake
#'
#' Wrapper for the ducklake [ATTACH](https://ducklake.select/docs/stable/duckdb/usage/connecting) command.
#' Creates a new DuckLake if the specified name does not exist, or connects to
#' an existing one. The lake can be detached with [detach_ducklake()].
#'
#' By default DuckDB is used as the catalog database. Alternative backends
#' (PostgreSQL, SQLite, MySQL) can be selected with the `backend` parameter,
#' which enables concurrent multi-client access.
#' See \url{https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database}.
#'
#' @param ducklake_name Name for the ducklake, used as the database alias in DuckDB
#' @param lake_path Directory path where the lake lives. For `"duckdb"` this is
#'   where the catalog file and Parquet data are stored. For other backends this
#'   sets the Parquet data location (DuckLake's `DATA_PATH`), which may also be
#'   an object-storage URI such as `"s3://bucket/path"` -- register credentials
#'   first with [create_storage_secret()]. (The `"duckdb"` backend needs a local
#'   `lake_path`, since its catalog is a database file.)
#' @param backend Catalog backend: `"duckdb"` (default), `"postgres"`,
#'   `"sqlite"`, or `"mysql"`.
#' @param catalog_connection_string Backend-specific connection string:
#'   \describe{
#'     \item{`"duckdb"`}{Not required. Defaults to `{ducklake_name}.ducklake`.}
#'     \item{`"postgres"`}{libpq string, e.g. `"dbname=mydb host=localhost"`.}
#'     \item{`"sqlite"`}{Path to the SQLite file, e.g. `"metadata.sqlite"`.}
#'     \item{`"mysql"`}{MySQL connection string, e.g. `"db=mydb host=localhost"`.}
#'   }
#' @param read_only Attach in read-only mode (default `FALSE`).
#' @param override_data_path Override the stored DATA_PATH in the catalog
#'   (default `FALSE`). Needed when restoring a backup to a different location.
#' @param data_inlining_row_limit Optional integer. Sets the per-connection
#'   data inlining row limit. Inserts or deletes affecting fewer rows than
#'   this threshold are stored directly in the catalog instead of writing
#'   Parquet files. The default (when `NULL`) uses the DuckLake default of 10
#'   rows. Set to `0` to disable inlining for this connection. This setting is
#'   not persisted; use [set_inlining_row_limit()] for persistent overrides.
#' @param encrypted If `TRUE`, DuckLake encrypts the Parquet data files it
#'   writes. Encryption keys are stored in the catalog database, so anyone
#'   with access to the catalog can read the data -- protect the catalog
#'   accordingly. Only applies when the lake is first created; an existing
#'   lake keeps the setting it was created with. The httpfs extension is
#'   loaded automatically: on some platforms (notably Windows) DuckDB's
#'   built-in crypto module is read-only and httpfs provides the writer.
#'   Default `FALSE`.
#' @param snapshot_version Optional snapshot id. Attaches the lake pinned to
#'   that snapshot: queries see the lake exactly as it was then, and writes
#'   are rejected. Mutually exclusive with `snapshot_time`.
#' @param snapshot_time Optional POSIXct or UTC timestamp string. Attaches
#'   the lake pinned to its state at that moment. Mutually exclusive with
#'   `snapshot_version`.
#'
#' @details
#' For credential management with PostgreSQL or MySQL, consider DuckDB's
#' built-in secrets manager instead of embedding credentials in the connection
#' string:
#'
#' \preformatted{conn <- get_ducklake_connection()
#' DBI::dbExecute(conn, "CREATE SECRET (
#'     TYPE postgres,
#'     HOST '127.0.0.1',
#'     PORT 5432,
#'     DATABASE ducklake_catalog,
#'     USER 'analyst',
#'     PASSWORD 'secret'
#' )")}
#'
#' Then pass an empty or partial `catalog_connection_string`; DuckDB fills in
#' the rest from the secret. See
#' \url{https://duckdb.org/docs/stable/configuration/secrets_manager}.
#'
#' **Windows limitation:** The `postgres` and `mysql` DuckDB extensions are not
#' available on Windows (MinGW toolchain). Only `duckdb` and `sqlite` backends
#' work there. Use Linux, macOS, or WSL for PostgreSQL/MySQL backends.
#' See \url{https://github.com/duckdb/duckdb/issues/7892}.
#'
#' @returns NULL
#' @family connection management
#' @export
#'
#' @seealso [detach_ducklake()], [install_ducklake()], [create_storage_secret()]
#'
#' @examples
#' \dontrun{
#' # DuckDB catalog (default)
#' attach_ducklake("my_lake", lake_path = "~/data/lake")
#'
#' # PostgreSQL catalog
#' attach_ducklake(
#'   "my_lake",
#'   backend = "postgres",
#'   catalog_connection_string = "dbname=ducklake_catalog host=localhost",
#'   lake_path = "/shared/lake/data/"
#' )
#'
#' # SQLite catalog
#' attach_ducklake(
#'   "my_lake",
#'   backend = "sqlite",
#'   catalog_connection_string = "metadata.sqlite",
#'   lake_path = "data_files/"
#' )
#'
#' # MySQL catalog
#' attach_ducklake(
#'   "my_lake",
#'   backend = "mysql",
#'   catalog_connection_string = "db=ducklake_catalog host=localhost",
#'   lake_path = "data_files/"
#' )
#'
#' # Custom inlining threshold for streaming workload
#' attach_ducklake(
#'   "streaming_lake",
#'   lake_path = "~/data/streaming",
#'   data_inlining_row_limit = 100
#' )
#'
#' # Encrypted Parquet files (keys live in the catalog)
#' attach_ducklake("secure_lake", lake_path = "~/data/secure", encrypted = TRUE)
#'
#' # A frozen view of the lake as of snapshot 12, e.g. for reproducing a report
#' attach_ducklake("lake_v12", lake_path = "~/data/lake", snapshot_version = 12)
#' }
attach_ducklake <- function(ducklake_name, lake_path,
                             backend = c("duckdb", "postgres", "sqlite", "mysql"),
                             catalog_connection_string = NULL,
                             read_only = FALSE,
                             override_data_path = FALSE,
                             data_inlining_row_limit = NULL,
                             encrypted = FALSE,
                             snapshot_version = NULL,
                             snapshot_time = NULL) {
  backend <- match.arg(backend)
  check_identifier(ducklake_name)

  if (!is.null(snapshot_version) && !is.null(snapshot_time)) {
    cli::cli_abort(
      "Provide only one of {.arg snapshot_version} and {.arg snapshot_time}."
    )
  }

  if (missing(lake_path) || is.null(lake_path)) {
    cli::cli_abort(c(
      "A {.arg lake_path} is required.",
      "i" = "This specifies the directory where the catalog and Parquet data files are stored.",
      "i" = "Example: {.code attach_ducklake(\"{ducklake_name}\", lake_path = \"path/to/lake\")}"
    ))
  }
  lake_path <- normalize_lake_path(lake_path)
  
  # Non-DuckDB backends also need a connection string
  if (backend != "duckdb") {
    if (is.null(catalog_connection_string)) {
      cli::cli_abort(c(
        "A {.arg catalog_connection_string} is required for the {.val {backend}} backend.",
        "i" = "See {.url https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database} for connection string formats."
      ))
    }
  }
  
  if (backend == "mysql") {
    cli::cli_warn(c(
      "MySQL has known issues as a DuckLake catalog backend.",
      "i" = "See {.url https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database#mysql} for details."
    ))
  }
  
  conn <- get_ducklake_connection()

  # Check if this ducklake is already attached to avoid conflicts
  # Query the list of attached databases
  attached <- tryCatch({
    DBI::dbGetQuery(conn, "SELECT database_name FROM duckdb_databases();")$database_name
  }, error = function(e) character(0))

  if (ducklake_name %in% attached) {
    # Already attached - just switch to it
    db_execute(sprintf("USE %s;", quote_ident(ducklake_name, conn)))
    register_lake(ducklake_name, backend, catalog_connection_string)
    return(invisible(NULL))
  }

  # Load required extensions (ducklake + backend-specific + crypto)
  ensure_extensions(backend, encrypted = encrypted)

  # Build and run the ATTACH command
  attach_sql <- build_attach_sql(ducklake_name, lake_path, backend,
                                  catalog_connection_string, read_only,
                                  override_data_path,
                                  data_inlining_row_limit,
                                  encrypted,
                                  snapshot_version,
                                  snapshot_time)
  db_execute(attach_sql)
  db_execute(sprintf("USE %s;", quote_ident(ducklake_name, conn)))
  register_lake(ducklake_name, backend, catalog_connection_string)

  invisible(NULL)
}

#' Collapse duplicate slashes in a local lake path
#'
#' DuckLake stores DATA_PATH verbatim and compares file paths as exact
#' strings, so a doubled slash -- which R's [tempdir()] produces on macOS --
#' makes every live data file look untracked to
#' `ducklake_delete_orphaned_files()`, turning orphan cleanup destructive.
#' Remote URIs (`s3://`, `gs://`, ...) are left untouched.
#'
#' @param lake_path Path as supplied by the user.
#' @returns The path with runs of `/` collapsed to one.
#' @noRd
normalize_lake_path <- function(lake_path) {
  if (grepl("^[A-Za-z][A-Za-z0-9+.-]*://", lake_path)) {
    return(lake_path)
  }
  gsub("/{2,}", "/", lake_path)
}

#' Install and load required DuckDB extensions for a given backend
#'
#' @param backend Catalog backend type
#' @param encrypted Whether the lake uses encrypted storage. Writing
#'   encrypted files requires the full crypto module from the httpfs
#'   extension on platforms where the built-in module is read-only
#'   (notably Windows).
#' @keywords internal
ensure_extensions <- function(backend, encrypted = FALSE) {
  tryCatch({
    db_execute("LOAD ducklake;")
  }, error = function(e) {
    db_execute("INSTALL ducklake;")
    db_execute("LOAD ducklake;")
  })

  if (encrypted) {
    tryCatch({
      db_execute("LOAD httpfs;")
    }, error = function(e) {
      tryCatch({
        db_execute("INSTALL httpfs;")
        db_execute("LOAD httpfs;")
      }, error = function(e2) {
        cli::cli_warn(c(
          "Could not load the {.pkg httpfs} extension: {e2$message}",
          "i" = "Writing encrypted files may fail where DuckDB's built-in crypto module is read-only (e.g., Windows)."
        ))
      })
    })
  }

  ext <- switch(backend,
    postgres = "postgres",
    sqlite = "sqlite",
    mysql = "mysql",
    NULL
  )

  if (!is.null(ext)) {
    tryCatch({
      db_execute(sprintf("LOAD %s;", ext))
    }, error = function(e) {
      db_execute(sprintf("INSTALL %s;", ext))
      db_execute(sprintf("LOAD %s;", ext))
    })
  }
}

#' Build the ATTACH SQL for a DuckLake
#'
#' @param ducklake_name Name for the ducklake alias
#' @param lake_path Path for data files
#' @param backend Catalog backend type
#' @param catalog_connection_string Backend-specific connection string
#' @param read_only Whether to attach in read-only mode
#' @param override_data_path Whether to add OVERRIDE_DATA_PATH TRUE
#' @param data_inlining_row_limit Optional integer for DATA_INLINING_ROW_LIMIT
#' @param encrypted Whether to add ENCRYPTED TRUE
#' @param snapshot_version Optional snapshot id for SNAPSHOT_VERSION
#' @param snapshot_time Optional timestamp for SNAPSHOT_TIME
#'
#' @returns A SQL ATTACH statement string
#' @keywords internal
build_attach_sql <- function(ducklake_name, lake_path, backend,
                              catalog_connection_string, read_only,
                              override_data_path = FALSE,
                              data_inlining_row_limit = NULL,
                              encrypted = FALSE,
                              snapshot_version = NULL,
                              snapshot_time = NULL) {
  connection_string <- switch(backend,
    duckdb = {
      ducklake_path <- file.path(lake_path, paste0(ducklake_name, ".ducklake"))
      sprintf("ducklake:%s", ducklake_path)
    },
    postgres = sprintf("ducklake:postgres:%s", catalog_connection_string),
    sqlite = sprintf("ducklake:sqlite:%s", catalog_connection_string),
    mysql = sprintf("ducklake:mysql:%s", catalog_connection_string)
  )
  
  options <- character()
  
  if (!is.null(lake_path)) {
    options <- c(options, sprintf("DATA_PATH '%s'", lake_path))
  }
  
  if (read_only) {
    options <- c(options, "READ_ONLY")
  }

  if (override_data_path) {
    options <- c(options, "OVERRIDE_DATA_PATH TRUE")
  }

  if (!is.null(data_inlining_row_limit)) {
    options <- c(options, sprintf("DATA_INLINING_ROW_LIMIT %d", as.integer(data_inlining_row_limit)))
  }

  if (encrypted) {
    options <- c(options, "ENCRYPTED TRUE")
  }

  if (!is.null(snapshot_version)) {
    options <- c(
      options,
      sprintf("SNAPSHOT_VERSION %d", as.integer(snapshot_version))
    )
  }

  if (!is.null(snapshot_time)) {
    options <- c(
      options,
      sprintf("SNAPSHOT_TIME %s", quote_sql(format_timestamp(snapshot_time)))
    )
  }

  if (length(options) > 0) {
    options_str <- paste(options, collapse = ", ")
    sprintf("ATTACH '%s' AS %s (%s);", connection_string, ducklake_name, options_str)
  } else {
    sprintf("ATTACH '%s' AS %s;", connection_string, ducklake_name)
  }
}
