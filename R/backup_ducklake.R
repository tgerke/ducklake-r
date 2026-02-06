#' Create a complete DuckLake backup
#'
#' Creates a timestamped backup of both the catalog database and data files.
#' The backup includes the complete state of the DuckLake at the time of backup,
#' allowing for point-in-time recovery.
#'
#' @param ducklake_name Name of the attached DuckLake
#' @param lake_path Path to the DuckLake directory containing the catalog file
#' @param backup_path Directory where backups should be stored. A timestamped
#'   subdirectory will be created within this path.
#'
#' @return Invisibly returns the path to the created backup directory
#' @export
#'
#' @details
#' The function creates a complete backup by:
#' \enumerate{
#'   \item Creating a timestamped backup directory
#'   \item Copying the catalog database file (.ducklake)
#'   \item Copying all data files from the main/ directory
#' }
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
#' # To restore from backup:
#' # detach_ducklake("my_lake")
#' # attach_ducklake("my_lake", lake_path = backup_dir)
#' }
backup_ducklake <- function(ducklake_name, lake_path, backup_path) {
  # Validate inputs
  if (!is.character(ducklake_name) || length(ducklake_name) != 1) {
    stop("ducklake_name must be a single character string")
  }
  if (!dir.exists(lake_path)) {
    stop("lake_path does not exist: ", lake_path)
  }
  
  # Create backup directory with timestamp
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  backup_dir <- file.path(backup_path, paste0("backup_", timestamp))
  dir.create(backup_dir, recursive = TRUE, showWarnings = FALSE)
  
  # Backup catalog
  catalog_file <- file.path(lake_path, paste0(ducklake_name, ".ducklake"))
  if (file.exists(catalog_file)) {
    file.copy(
      from = catalog_file,
      to = file.path(backup_dir, paste0(ducklake_name, ".ducklake"))
    )
    message("Catalog backed up successfully")
  } else {
    warning("Catalog file not found: ", catalog_file)
  }
  
  # Backup data directory
  main_dir <- file.path(lake_path, "main")
  if (dir.exists(main_dir)) {
    fs::dir_copy(
      path = main_dir,
      new_path = file.path(backup_dir, "main")
    )
    message("Data files backed up successfully")
  } else {
    warning("Data directory not found: ", main_dir)
  }
  
  message(sprintf("Backup completed: %s", backup_dir))
  invisible(backup_dir)
}
