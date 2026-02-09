# Helper functions for setting up test environments
# This file is automatically loaded by testthat

#' Get the package environment for tests
get_ducklake_env <- function() {
  asNamespace("ducklake")[[".ducklake_env"]]
}

#' Create a temporary ducklake for testing
#'
#' Creates a test ducklake in a temporary directory with proper DuckLake catalog
#' 
#' @return List with ducklake_name, temp_dir, lake_path, and conn
create_temp_ducklake <- function() {
  temp_dir <- tempfile()
  dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Use simple name without special characters that might cause issues
  ducklake_name <- paste0("testlake_", format(Sys.time(), "%Y%m%d%H%M%S"))
  
  # Attach the ducklake - this creates the DuckLake catalog
  attach_ducklake(ducklake_name, lake_path = temp_dir)
  
  # Get the connection
  conn <- get_ducklake_connection()
  
  list(
    ducklake_name = ducklake_name,
    temp_dir = temp_dir,
    lake_path = temp_dir,
    conn = conn
  )
}

#' Clean up temporary ducklake files
#'
#' @param test_lake Output from create_temp_ducklake()
cleanup_temp_ducklake <- function(test_lake) {
  # Detach the ducklake properly
  tryCatch({
    detach_ducklake(test_lake$ducklake_name)
  }, error = function(e) {
    # Ignore errors during cleanup
  })
  
  # Clean up files
  if (!is.null(test_lake$temp_dir) && dir.exists(test_lake$temp_dir)) {
    unlink(test_lake$temp_dir, recursive = TRUE)
  }
}
