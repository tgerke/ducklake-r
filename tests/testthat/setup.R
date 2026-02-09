# Setup file - runs once before all tests
# Install ducklake extension globally for all tests
suppressMessages({
  tryCatch({
    ducklake::install_ducklake()
  }, error = function(e) {
    # Extension might already be installed, that's ok
    NULL
  })
})
