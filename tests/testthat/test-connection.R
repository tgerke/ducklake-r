test_that("get_ducklake_connection lazily creates a valid connection", {
  skip_if_not_installed("duckdb")

  conn <- get_ducklake_connection()
  expect_s4_class(conn, "duckdb_connection")
  expect_true(DBI::dbIsValid(conn))

  # Repeated calls return the same connection
  expect_identical(conn, get_ducklake_connection())
})

test_that("connection is file-backed with a temp spill directory", {
  skip_if_not_installed("duckdb")

  conn <- get_ducklake_connection()
  settings <- DBI::dbGetQuery(
    conn,
    "SELECT value FROM duckdb_settings() WHERE name = 'temp_directory'"
  )
  expect_equal(nrow(settings), 1)
  expect_true(nzchar(settings$value))
})

test_that("shutdown via detach_ducklake releases and lazily recreates", {
  skip_if_not_installed("duckdb")

  conn_before <- get_ducklake_connection()
  detach_ducklake(shutdown = TRUE)
  expect_false(DBI::dbIsValid(conn_before))

  # Next access transparently creates a fresh connection
  conn_after <- get_ducklake_connection()
  expect_true(DBI::dbIsValid(conn_after))
  expect_false(identical(conn_before, conn_after))
})

test_that("set_ducklake_connection registers a user connection and never closes it", {
  skip_if_not_installed("duckdb")

  user_conn <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(user_conn, shutdown = TRUE), add = TRUE)

  set_ducklake_connection(user_conn)
  expect_identical(get_ducklake_connection(), user_conn)

  # detach with shutdown must warn and leave the user connection open
  expect_warning(
    detach_ducklake(shutdown = TRUE),
    "not shut down"
  )
  expect_true(DBI::dbIsValid(user_conn))

  # Restore package-owned state for subsequent tests: drop the injected
  # connection so the next access creates a fresh owned one
  .ducklake_env$conn <- NULL
  .ducklake_env$conn_owned <- NULL
  expect_true(DBI::dbIsValid(get_ducklake_connection()))
})

test_that("set_ducklake_connection rejects invalid input", {
  skip_if_not_installed("duckdb")

  expect_error(set_ducklake_connection("not a connection"), "must be a DuckDB connection")

  closed <- DBI::dbConnect(duckdb::duckdb())
  DBI::dbDisconnect(closed, shutdown = TRUE)
  expect_error(set_ducklake_connection(closed), "not a valid")
})

test_that("shutdown releases DuckLake catalog file locks", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("fs")

  temp_dir <- tempfile()
  dir.create(temp_dir, recursive = TRUE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  attach_ducklake("locktest", lake_path = temp_dir)
  create_table(mtcars, "cars")

  detach_ducklake("locktest", shutdown = TRUE)

  # With locks released, the catalog file must be independently openable
  catalog_file <- file.path(temp_dir, "locktest.ducklake")
  expect_true(file.exists(catalog_file))
  probe <- DBI::dbConnect(duckdb::duckdb(dbdir = catalog_file))
  tables <- DBI::dbListTables(probe)
  DBI::dbDisconnect(probe, shutdown = TRUE)
  expect_true(length(tables) > 0)
})
