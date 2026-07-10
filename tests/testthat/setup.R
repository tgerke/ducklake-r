# Setup file - runs once before all tests
# Install the ducklake extension for the local/CI test runs that use it.
# Skipped on CRAN: installing an extension needs network access, and every
# test that would use it skips itself via skip_if_no_ducklake().
if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  suppressMessages({
    tryCatch({
      ducklake::install_ducklake()
    }, error = function(e) {
      # Extension might already be installed, that's ok
      NULL
    })
  })
}
