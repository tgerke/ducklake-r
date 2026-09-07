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
#' The ducklake extension, and the extension for a non-DuckDB backend, are
#' loaded on each attach and downloaded the first time they are needed; a
#' message names the extension and the directory before any download. Where
#' that directory is depends on duckdb (see `?duckdb::duckdb_storage`): in an
#' interactive session duckdb offers to create `~/.duckdb` the first time it
#' connects, and with that the download happens once per machine. To fetch
#' the extensions ahead of time, for a container image or a machine that is
#' offline when the lake is attached, use [install_ducklake()].
#'
#' @param ducklake_name Name for the ducklake, used as the database alias in DuckDB
#' @param lake_path Directory where the Parquet data files are stored
#'   (DuckLake's `DATA_PATH`). May be a local directory, created if it does
#'   not exist yet, or an object-storage URI such as `"s3://bucket/path"`
#'   -- register credentials first with [create_storage_secret()]. For `"duckdb"` the catalog file lives in this
#'   directory too by default; give `catalog_connection_string` to place it
#'   elsewhere, which is how a local catalog pairs with remote data.
#' @param backend Catalog backend: `"duckdb"` (default), `"postgres"`,
#'   `"sqlite"`, or `"mysql"`.
#' @param catalog_connection_string Backend-specific connection string:
#'   \describe{
#'     \item{`"duckdb"`}{Optional path for the catalog database file.
#'       Defaults to `{lake_path}/{ducklake_name}.ducklake`. Set it to keep
#'       the catalog on local disk while `lake_path` points at object
#'       storage.}
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
#' @param meta_encryption_key Optional key that encrypts the catalog
#'   database file itself, with AES-256-GCM (`"duckdb"` backend only; the
#'   `META_` prefix is how DuckLake forwards the option to the metadata
#'   catalog). The key takes effect when the catalog is first created -- an
#'   existing unencrypted catalog cannot be encrypted after the fact -- and
#'   the same key is required on every later attach, with no recovery if it
#'   is lost. Pairs naturally with `encrypted`, whose Parquet keys are
#'   stored in the catalog. Pass `askpass::askpass()` to be prompted rather
#'   than putting the key in code. The key is interpolated into the
#'   `ATTACH` statement text, so it can surface where statements are logged
#'   or profiled.
#' @param snapshot_version Optional snapshot id. Attaches the lake pinned to
#'   that snapshot: queries see the lake exactly as it was then, and writes
#'   are rejected. Mutually exclusive with `snapshot_time`.
#' @param snapshot_time Optional POSIXct or UTC timestamp string. Attaches
#'   the lake pinned to its state at that moment. Mutually exclusive with
#'   `snapshot_version`.
#' @param automatic_migration If `TRUE`, let DuckLake upgrade a catalog
#'   written in an earlier format version to the one the installed
#'   extension uses (the `AUTOMATIC_MIGRATION` option). Needed once for a
#'   lake created with DuckDB 1.5.1, whose extension wrote the 0.4 format,
#'   after upgrading to 1.5.2 or later. The upgrade is permanent, so take a
#'   backup first. Default `FALSE`, in which case a mismatch is an error.
#' @param create Create the lake when none exists at the catalog location
#'   (default `TRUE`, DuckLake's `CREATE_IF_NOT_EXISTS`). Pass `FALSE` when
#'   you mean to open an existing lake, so a mistyped path or name is an
#'   error rather than a new, empty lake.
#' @param metadata_schema Optional schema inside the catalog database that
#'   holds this lake's metadata tables (DuckLake's `METADATA_SCHEMA`,
#'   default `main`). Lets several lakes share one PostgreSQL database,
#'   each in its own schema.
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
#' See \url{https://github.com/duckdb/duckdb/issues/7892}. The `aws` and
#' `azure` extensions are missing on Windows too, so
#' [create_storage_secret()] with `provider = "credential_chain"` or
#' `type = "azure"` does not work there; explicit S3 keys do.
#'
#' @returns Invisibly, `NULL`. Called for its side effect of attaching the
#'   DuckLake catalog to the package's DuckDB connection.
#' @family connection management
#' @export
#'
#' @seealso [detach_ducklake()], [install_ducklake()], [create_storage_secret()]
#'
#' @examplesIf ducklake_extension_available()
#' # DuckDB catalog (default)
#' lake_dir <- tempfile("my_lake_")
#' dir.create(lake_dir)
#' attach_ducklake("my_lake", lake_path = lake_dir)
#' detach_ducklake("my_lake")
#'
#' # Custom inlining threshold for a streaming workload
#' stream_dir <- tempfile("streaming_lake_")
#' dir.create(stream_dir)
#' attach_ducklake(
#'   "streaming_lake",
#'   lake_path = stream_dir,
#'   data_inlining_row_limit = 100
#' )
#'
#' detach_ducklake("streaming_lake", shutdown = TRUE)
#' unlink(c(lake_dir, stream_dir), recursive = TRUE)
#'
#' # The remaining forms need a catalog server, or extensions that are
#' # downloaded on first use, so they are not run here.
#' \dontrun{
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
#' # DuckDB catalog on local disk, Parquet data on S3
#' create_storage_secret("s3", provider = "credential_chain")
#' attach_ducklake(
#'   "trial_lake",
#'   lake_path = "s3://my-trial-lake/data",
#'   catalog_connection_string = "trial_lake.ducklake"
#' )
#'
#' # Read-only attach of a .ducklake catalog straight from object storage
#' attach_ducklake(
#'   "trial_lake",
#'   lake_path = "s3://my-trial-lake/data",
#'   catalog_connection_string = "s3://my-trial-lake/trial_lake.ducklake",
#'   read_only = TRUE
#' )
#'
#' # Encrypted Parquet files (keys live in the catalog); needs httpfs
#' attach_ducklake("secure_lake", lake_path = "path/to/lake", encrypted = TRUE)
#'
#' # Encrypt the catalog database too -- it is where the Parquet keys live.
#' # askpass prompts for the key so it never sits in code.
#' attach_ducklake(
#'   "secure_lake",
#'   lake_path = "path/to/lake",
#'   encrypted = TRUE,
#'   meta_encryption_key = askpass::askpass("Catalog encryption key")
#' )
#'
#' # A frozen view of the lake as of snapshot 12, e.g. to reproduce a report
#' attach_ducklake("lake_v12", lake_path = "path/to/lake", snapshot_version = 12)
#'
#' # A lake created with DuckDB 1.5.1 (catalog format 0.4), opened after
#' # upgrading: migrate it once, then attach as usual
#' attach_ducklake("old_lake", lake_path = "path/to/lake", automatic_migration = TRUE)
#'
#' # Open an existing lake, and error if it is not there
#' attach_ducklake("prod_lake", lake_path = "/lakes/prod", create = FALSE)
#'
#' # Several lakes in one PostgreSQL database, one schema each
#' attach_ducklake(
#'   "study_a",
#'   backend = "postgres",
#'   catalog_connection_string = "dbname=lakes host=db.example.org",
#'   lake_path = "s3://lakes/study_a",
#'   metadata_schema = "study_a"
#' )
#' }
attach_ducklake <- function(ducklake_name, lake_path,
                             backend = c("duckdb", "postgres", "sqlite", "mysql"),
                             catalog_connection_string = NULL,
                             read_only = FALSE,
                             override_data_path = FALSE,
                             data_inlining_row_limit = NULL,
                             encrypted = FALSE,
                             meta_encryption_key = NULL,
                             snapshot_version = NULL,
                             snapshot_time = NULL,
                             automatic_migration = FALSE,
                             create = TRUE,
                             metadata_schema = NULL) {
  backend <- match.arg(backend)
  check_identifier(ducklake_name)

  if (!is.null(snapshot_version) && !is.null(snapshot_time)) {
    cli::cli_abort(
      "Provide only one of {.arg snapshot_version} and {.arg snapshot_time}."
    )
  }

  if (!is.null(meta_encryption_key)) {
    if (backend != "duckdb") {
      cli::cli_abort(c(
        "{.arg meta_encryption_key} is only supported for the {.val duckdb} backend.",
        "i" = "It encrypts the DuckDB catalog database file. A {.val {backend}} catalog is encrypted with that database's own tools."
      ))
    }
    if (!is.character(meta_encryption_key) ||
        length(meta_encryption_key) != 1 ||
        is.na(meta_encryption_key) ||
        !nzchar(meta_encryption_key)) {
      cli::cli_abort(
        "{.arg meta_encryption_key} must be a single, non-empty string."
      )
    }
  }

  if (missing(lake_path) || is.null(lake_path)) {
    cli::cli_abort(c(
      "A {.arg lake_path} is required.",
      "i" = "This specifies the directory where the catalog and Parquet data files are stored.",
      "i" = "Example: {.code attach_ducklake(\"{ducklake_name}\", lake_path = \"path/to/lake\")}"
    ))
  }
  lake_path <- normalize_lake_path(lake_path)

  # DuckDB cannot create a database file in a directory that does not
  # exist, so when creating the lake is allowed, create the local lake
  # directory (and the directory of a local catalog file) on demand
  if (isTRUE(create)) {
    ensure_local_dir(lake_path)
    if (backend %in% c("duckdb", "sqlite") && !is.null(catalog_connection_string)) {
      ensure_local_dir(dirname(catalog_connection_string))
    }
  }

  # Non-DuckDB backends also need a connection string
  if (backend != "duckdb") {
    if (is.null(catalog_connection_string)) {
      cli::cli_abort(c(
        "A {.arg catalog_connection_string} is required for the {.val {backend}} backend.",
        "i" = "See {.url https://ducklake.select/docs/stable/duckdb/usage/choosing_a_catalog_database} for connection string formats."
      ))
    }
  }
  
  # DuckDB cannot create or write a database file on object storage, so a
  # writable duckdb-backend lake needs its catalog on local disk
  if (backend == "duckdb" && !read_only) {
    catalog_path <- if (!is.null(catalog_connection_string)) {
      catalog_connection_string
    } else {
      file.path(lake_path, paste0(ducklake_name, ".ducklake"))
    }
    if (is_remote_path(catalog_path)) {
      cli::cli_abort(c(
        "DuckDB cannot write its catalog file to object storage.",
        "x" = "The catalog would live at {.val {catalog_path}}.",
        "i" = "Pass {.arg catalog_connection_string} with a local path for the catalog file; {.arg lake_path} then only sets where the Parquet data goes.",
        "i" = "Or attach an existing remote catalog with {.code read_only = TRUE}.",
        "i" = "Or pick a {.arg backend} whose catalog lives elsewhere, such as {.val sqlite} or {.val postgres}."
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

  # Load required extensions (ducklake + backend-specific + crypto/remote IO)
  ensure_extensions(
    backend,
    encrypted = encrypted || !is.null(meta_encryption_key),
    remote = is_remote_path(lake_path) ||
      (!is.null(catalog_connection_string) &&
         is_remote_path(catalog_connection_string))
  )

  # Build and run the ATTACH command
  attach_sql <- build_attach_sql(ducklake_name, lake_path, backend,
                                  catalog_connection_string, read_only,
                                  override_data_path,
                                  data_inlining_row_limit,
                                  encrypted,
                                  meta_encryption_key,
                                  snapshot_version,
                                  snapshot_time,
                                  automatic_migration,
                                  create,
                                  metadata_schema)
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
  if (is_remote_path(lake_path)) {
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
#' @param remote Whether the data path or catalog path is a remote URI, in
#'   which case httpfs handles the IO. Loading it up front beats relying on
#'   DuckDB's mid-statement autoload, which can fail.
#' @returns Invisibly, `NULL`. Called for its side effect of loading (and, if
#'   necessary, installing) the required DuckDB extensions.
#' @keywords internal
ensure_extensions <- function(backend, encrypted = FALSE, remote = FALSE) {
  load_or_install_extension("ducklake")

  if (encrypted || remote) {
    tryCatch(
      load_or_install_extension("httpfs"),
      error = function(e) {
        cli::cli_warn(c(
          "Could not load the {.pkg httpfs} extension: {e$message}",
          "i" = if (encrypted) {
            "Writing encrypted files may fail where DuckDB's built-in crypto module is read-only (e.g., Windows)."
          } else {
            "Remote paths will only work if DuckDB manages to autoload httpfs itself."
          }
        ))
      }
    )
  }

  ext <- switch(backend,
    postgres = "postgres",
    sqlite = "sqlite",
    mysql = "mysql",
    NULL
  )

  if (!is.null(ext)) {
    load_or_install_extension(ext)
  }
}

#' Build the ATTACH SQL for a DuckLake
#'
#' @param ducklake_name Name for the ducklake alias
#' @param lake_path Path for data files
#' @param backend Catalog backend type
#' @param catalog_connection_string Backend-specific connection string; for
#'   the duckdb backend, an optional path for the catalog file
#' @param read_only Whether to attach in read-only mode
#' @param override_data_path Whether to add OVERRIDE_DATA_PATH TRUE
#' @param data_inlining_row_limit Optional integer for DATA_INLINING_ROW_LIMIT
#' @param encrypted Whether to add ENCRYPTED TRUE
#' @param meta_encryption_key Optional key for META_ENCRYPTION_KEY
#' @param snapshot_version Optional snapshot id for SNAPSHOT_VERSION
#' @param snapshot_time Optional timestamp for SNAPSHOT_TIME
#' @param automatic_migration Whether to add AUTOMATIC_MIGRATION
#' @param create Whether to allow creating the lake (CREATE_IF_NOT_EXISTS)
#' @param metadata_schema Optional schema for METADATA_SCHEMA
#'
#' @returns A SQL ATTACH statement string
#' @keywords internal
build_attach_sql <- function(ducklake_name, lake_path, backend,
                              catalog_connection_string, read_only,
                              override_data_path = FALSE,
                              data_inlining_row_limit = NULL,
                              encrypted = FALSE,
                              meta_encryption_key = NULL,
                              snapshot_version = NULL,
                              snapshot_time = NULL,
                              automatic_migration = FALSE,
                              create = TRUE,
                              metadata_schema = NULL) {
  connection_string <- switch(backend,
    duckdb = {
      ducklake_path <- if (!is.null(catalog_connection_string)) {
        catalog_connection_string
      } else {
        file.path(lake_path, paste0(ducklake_name, ".ducklake"))
      }
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

  if (!is.null(meta_encryption_key)) {
    options <- c(
      options,
      sprintf("META_ENCRYPTION_KEY %s", quote_sql(meta_encryption_key))
    )
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

  if (automatic_migration) {
    options <- c(options, "AUTOMATIC_MIGRATION")
  }

  if (!isTRUE(create)) {
    options <- c(options, "CREATE_IF_NOT_EXISTS false")
  }

  if (!is.null(metadata_schema)) {
    check_identifier(metadata_schema, arg = "metadata_schema")
    options <- c(options, sprintf("METADATA_SCHEMA %s", quote_sql(metadata_schema)))
  }

  if (length(options) > 0) {
    options_str <- paste(options, collapse = ", ")
    sprintf("ATTACH '%s' AS %s (%s);", connection_string, ducklake_name, options_str)
  } else {
    sprintf("ATTACH '%s' AS %s;", connection_string, ducklake_name)
  }
}
