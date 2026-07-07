#' Create a DuckLake backup
#'
#' Creates a timestamped backup of the Parquet data files and, for file-based
#' backends (DuckDB, SQLite), the catalog database file. For PostgreSQL/MySQL
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
#' @export
#'
#' @details
#' For file-based backends the DuckLake is temporarily detached during backup
#' to release file locks and ensure a consistent copy. It is automatically
#' re-attached afterwards.
#'
#' **Important notes:**
#' \itemize{
#'   \item Transactions committed after a backup won't be tracked when recovering.
#'     The data will exist in the Parquet files, but the backup will point to
#'     an earlier snapshot.
#'   \item Consider coordinating backups with maintenance operations (compaction
#'     and cleanup) for optimal storage efficiency.
#'   \item For production systems, schedule backups using \code{{cronR}} or
#'     \code{{taskscheduleR}}.
#' }
#'
#' @examples
#' \dontrun{
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
#' # Create a backup
#' backup_dir <- backup_ducklake(
#'   ducklake_name = "my_lake",
#'   lake_path = lake_dir,
#'   backup_path = file.path(lake_dir, "backups")
#' )
#'
#' # Restore (override_data_path needed when location differs):
#' # detach_ducklake("my_lake")
#' # attach_ducklake("my_lake", lake_path = backup_dir, override_data_path = TRUE)
#' }
backup_ducklake <- function(ducklake_name, lake_path, backup_path) {
  # Validate inputs
  if (!is.character(ducklake_name) || length(ducklake_name) != 1) {
    cli::cli_abort("{.arg ducklake_name} must be a single character string.")
  }
  if (!dir.exists(lake_path)) {
    cli::cli_abort("{.arg lake_path} does not exist: {.path {lake_path}}")
  }

  backend <- get_ducklake_backend(ducklake_name)

  # Create backup directory with timestamp
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  backup_dir <- file.path(backup_path, paste0("backup_", timestamp))
  dir.create(backup_dir, recursive = TRUE, showWarnings = FALSE)

  # File-based backends: shut down to release file locks, copy, re-attach
  if (backend %in% c("duckdb", "sqlite")) {
    catalog_file <- if (backend == "duckdb") {
      file.path(lake_path, paste0(ducklake_name, ".ducklake"))
    } else {
      .ducklake_env$lakes[[ducklake_name]]$catalog_connection_string
    }

    if (!is.null(catalog_file) && file.exists(catalog_file)) {
      detach_ducklake(ducklake_name, shutdown = TRUE)

      copy_ok <- file.copy(
        from = catalog_file,
        to = file.path(backup_dir, basename(catalog_file))
      )

      if (backend == "duckdb") {
        attach_ducklake(ducklake_name, lake_path = lake_path, backend = backend)
      } else {
        attach_ducklake(ducklake_name, lake_path = lake_path, backend = backend,
                        catalog_connection_string = catalog_file)
      }

      dest_file <- file.path(backup_dir, basename(catalog_file))
      if (copy_ok && file.size(dest_file) > 0) {
        cli::cli_inform("Catalog backed up successfully.")
      } else {
        cli::cli_warn(c(
          "Catalog file could not be copied (likely locked by DuckDB).",
          "i" = "Data files were still backed up."
        ))
        # Remove the 0-byte file so it doesn't look like a valid backup
        unlink(dest_file)
      }
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
      fs::dir_copy(path = d, new_path = file.path(backup_dir, basename(d)))
    }
    cli::cli_inform("Data files backed up successfully ({length(data_dirs)} director{?y/ies}).")
  } else {
    cli::cli_warn("No data directories found in {.path {lake_path}}.")
  }

  cli::cli_inform("Backup completed: {.path {backup_dir}}")
  invisible(backup_dir)
}
