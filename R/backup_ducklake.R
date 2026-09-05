#' Create a DuckLake backup
#'
#' Creates a timestamped backup of the Parquet data files and, for file-based
#' backends (DuckDB, SQLite), the catalog database. For PostgreSQL/MySQL
#' backends only data files are copied; use `pg_dump` / `mysqldump` for the
#' catalog.
#'
#' @param ducklake_name Name of the attached DuckLake
#' @param lake_path Path to the DuckLake directory containing the data files
#'   (and catalog file for DuckDB/SQLite backends)
#' @param backup_path Directory where backups should be stored. A timestamped
#'   subdirectory will be created within this path.
#'
#' @returns Invisibly returns the path to the created backup directory
#' @family maintenance
#' @export
#'
#' @details
#' The catalog is copied with DuckDB's `COPY FROM DATABASE` while the lake
#' stays attached. The copy is taken inside one transaction, so it is a
#' consistent snapshot of the metadata, and nothing is detached or shut
#' down along the way: other attached lakes, in-memory secrets, and a
#' connection you registered with [set_ducklake_connection()] are left as
#' they are. The data directories (one per schema) are copied as files.
#'
#' To work with the backup, attach it with `lake_path` pointing at the
#' backup directory. Pass `override_data_path = TRUE`, since the copied
#' catalog remembers the original data location, and `create = FALSE`, so a
#' mistyped path is an error rather than a new, empty lake. When the
#' catalog file was not named after the lake (a split layout), name it with
#' `catalog_connection_string`.
#'
#' **Important notes:**
#' \itemize{
#'   \item Transactions committed after a backup won't be tracked when recovering.
#'     The data will exist in the Parquet files, but the backup will point to
#'     an earlier snapshot.
#'   \item Run compaction and cleanup before a backup, not after: they
#'     rewrite and remove data files that the copied catalog refers to.
#'   \item For production systems, schedule backups using \code{{cronR}} or
#'     \code{{taskscheduleR}}.
#' }
#'
#' @examplesIf ducklake_extension_available()
#' # Create a DuckLake
#' lake_dir <- tempfile("my_lake")
#' dir.create(lake_dir)
#' attach_ducklake("my_lake", lake_path = lake_dir)
#'
#' # Add some data
#' with_transaction(
#'   create_table(mtcars, "cars"),
#'   author = "User",
#'   commit_message = "Initial data"
#' )
#'
#' # Create a backup; the lake stays attached throughout
#' backup_dir <- backup_ducklake(
#'   ducklake_name = "my_lake",
#'   lake_path = lake_dir,
#'   backup_path = file.path(lake_dir, "backups")
#' )
#'
#' # Restore (override_data_path needed when location differs):
#' # detach_ducklake("my_lake")
#' # attach_ducklake("my_lake", lake_path = backup_dir,
#' #                 override_data_path = TRUE, create = FALSE)
#'
#' detach_ducklake("my_lake", shutdown = TRUE)
#' unlink(lake_dir, recursive = TRUE)
backup_ducklake <- function(ducklake_name, lake_path, backup_path) {
  # Validate inputs
  if (!is.character(ducklake_name) || length(ducklake_name) != 1) {
    cli::cli_abort("{.arg ducklake_name} must be a single character string.")
  }
  if (is_remote_path(lake_path)) {
    cli::cli_abort(c(
      "{.fn backup_ducklake} only supports local data paths.",
      "x" = "Got remote {.arg lake_path} {.val {lake_path}}.",
      "i" = "For object storage, use your provider's replication or sync tooling (e.g. bucket versioning, {.code aws s3 sync})."
    ))
  }
  if (!dir.exists(lake_path)) {
    cli::cli_abort("{.arg lake_path} does not exist: {.path {lake_path}}")
  }

  backend <- get_ducklake_backend(ducklake_name)
  conn <- get_ducklake_connection()

  # Create backup directory with timestamp
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  backup_dir <- file.path(backup_path, paste0("backup_", timestamp))
  dir.create(backup_dir, recursive = TRUE, showWarnings = FALSE)

  # File-based backends: copy the catalog database through DuckDB, with
  # the lake still attached. The registry holds the connection string for
  # a duckdb catalog that lives outside lake_path, NULL for the default
  # layout.
  if (backend %in% c("duckdb", "sqlite")) {
    stored_catalog <- .ducklake_env$lakes[[ducklake_name]]$catalog_connection_string
    catalog_file <- if (backend == "duckdb" && is.null(stored_catalog)) {
      file.path(lake_path, paste0(ducklake_name, ".ducklake"))
    } else {
      stored_catalog
    }

    if (!is.null(catalog_file) && file.exists(catalog_file)) {
      copy_catalog_database(
        ducklake_name, backend,
        file.path(backup_dir, basename(catalog_file)), conn
      )
      dl_inform("Catalog backed up successfully.")
    } else {
      cli::cli_warn("Catalog file not found: {.path {catalog_file}}")
    }
  } else {
    tool <- if (backend == "postgres") "pg_dump" else "mysqldump"
    cli::cli_warn(c(
      "Catalog backup is not included for the {.val {backend}} backend.",
      "i" = "Use {.code {tool}} to backup the catalog database separately.",
      "i" = "Only Parquet data files will be backed up."
    ))
  }

  # Backup the data directories. DuckLake creates one directory per schema
  # (usually just "main", but any additional schemas live alongside it), so
  # enumerate rather than assume. The backup destination is excluded in case
  # it lives inside the lake path.
  data_dirs <- list.dirs(lake_path, recursive = FALSE)
  norm_backup <- normalizePath(backup_dir, mustWork = FALSE)
  is_backup_dest <- vapply(
    data_dirs,
    function(d) startsWith(norm_backup, normalizePath(d, mustWork = FALSE)),
    logical(1)
  )
  data_dirs <- data_dirs[!is_backup_dest]

  if (length(data_dirs) > 0) {
    for (d in data_dirs) {
      # file.copy(recursive = TRUE) copies `d` *into* backup_dir, preserving
      # its basename
      file.copy(from = d, to = backup_dir, recursive = TRUE)
    }
    dl_inform("Data files backed up successfully ({length(data_dirs)} director{?y/ies}).")
  } else {
    cli::cli_warn("No data directories found in {.path {lake_path}}.")
  }

  dl_inform("Backup completed: {.path {backup_dir}}")
  invisible(backup_dir)
}

#' Copy a lake's metadata catalog into a new database file
#'
#' `COPY FROM DATABASE` reads the attached metadata catalog inside one
#' transaction, so the copy is consistent, and the lake stays attached: no
#' file locks to release and no shutdown. A SQLite catalog is copied into a
#' SQLite file so the backup attaches with the same backend.
#'
#' @param ducklake_name The lake whose catalog to copy.
#' @param backend `"duckdb"` or `"sqlite"`.
#' @param dest_file Path of the new database file (overwritten).
#' @param conn A DBI connection.
#' @returns Invisibly, `dest_file`.
#' @noRd
copy_catalog_database <- function(ducklake_name, backend, dest_file, conn) {
  meta_db <- quote_ident(paste0("__ducklake_metadata_", ducklake_name), conn)
  alias <- "__ducklake_backup"
  unlink(dest_file)

  try(db_execute(sprintf("DETACH %s;", alias), conn = conn), silent = TRUE)
  db_execute(
    sprintf(
      "ATTACH %s AS %s%s;",
      quote_sql(dest_file), alias,
      if (backend == "sqlite") " (TYPE sqlite)" else ""
    ),
    conn = conn
  )
  on.exit(
    try(db_execute(sprintf("DETACH %s;", alias), conn = conn), silent = TRUE),
    add = TRUE
  )
  db_execute(sprintf("COPY FROM DATABASE %s TO %s;", meta_db, alias), conn = conn)

  invisible(dest_file)
}
