# Helper functions for setting up test environments
# This file is automatically loaded by testthat

#' Get the package environment for tests
get_ducklake_env <- function() {
  asNamespace("ducklake")[[".ducklake_env"]]
}

#' Skip tests that need the ducklake DuckDB extension
#'
#' The extension is not bundled with the duckdb R package: installing it
#' needs network access on first use, so these tests cannot run on CRAN.
skip_if_no_ducklake <- function() {
  testthat::skip_on_cran()
  ok <- tryCatch(
    {
      DBI::dbExecute(ducklake::get_ducklake_connection(), "LOAD ducklake;")
      TRUE
    },
    error = function(e) FALSE
  )
  testthat::skip_if_not(ok, "ducklake extension not available")
  invisible(TRUE)
}

#' Create a temporary ducklake for testing
#'
#' Creates a test ducklake in a temporary directory with proper DuckLake catalog
#' 
#' @return List with ducklake_name, temp_dir, lake_path, and conn
create_temp_ducklake <- function() {
  skip_if_no_ducklake()

  temp_dir <- tempfile()
  dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Use simple name without special characters that might cause issues
  ducklake_name <- paste0(
    "testlake_",
    format(Sys.time(), "%Y%m%d%H%M%S"), "_",
    sample.int(.Machine$integer.max, 1)
  )
  
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
