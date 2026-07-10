# Tests for install_ducklake()

test_that("install_ducklake issues INSTALL statements for backends", {
  executed <- character()
  local_mocked_bindings(
    db_execute = function(sql, ...) {
      executed <<- c(executed, sql)
      invisible(NULL)
    }
  )

  suppressMessages(install_ducklake(backend = c("sqlite", "mysql")))

  expect_equal(
    executed,
    c("INSTALL ducklake;", "INSTALL sqlite;", "INSTALL mysql;")
  )
})

test_that("install_ducklake aborts on DuckDB engines older than 1.5.1", {
  local_mocked_bindings(
    duckdb_version_at_least = function(version, minimum) FALSE
  )

  expect_error(install_ducklake(), "requires DuckDB version 1.5.1")
})

test_that("install_ducklake rejects unknown backends", {
  local_mocked_bindings(db_execute = function(sql, ...) invisible(NULL))

  expect_error(
    suppressMessages(install_ducklake(backend = "oracle")),
    "should be one of"
  )
})
