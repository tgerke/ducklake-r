# Tests for create_storage_secret()

test_that("create_storage_secret registers a named, scoped s3 secret", {
  skip_if_not_installed("duckdb")

  conn <- get_ducklake_connection()
  loaded <- tryCatch(
    {
      DBI::dbExecute(conn, "LOAD httpfs;")
      TRUE
    },
    error = function(e) FALSE
  )
  skip_if_not(loaded, "httpfs extension not available")

  expect_message(
    create_storage_secret(
      "s3",
      key_id = "AKIAEXAMPLE",
      secret = "not-a-real-secret",
      region = "us-east-1",
      scope = "s3://example-bucket",
      name = "ducklake_test_secret"
    ),
    "storage secret"
  )

  secrets <- DBI::dbGetQuery(
    conn,
    "SELECT name, type FROM duckdb_secrets() WHERE name = 'ducklake_test_secret';"
  )
  expect_equal(nrow(secrets), 1)
  expect_equal(tolower(secrets$type), "s3")

  DBI::dbExecute(conn, "DROP SECRET ducklake_test_secret;")
})

test_that("create_storage_secret validates its inputs", {
  local_mocked_bindings(db_execute = function(sql, ...) invisible(NULL))

  expect_error(
    create_storage_secret("s3", "unnamed-value"),
    "must be named"
  )
  expect_error(
    create_storage_secret("s3", `bad-key` = "x"),
    "Invalid secret parameter name"
  )
  expect_error(
    create_storage_secret("s3", name = "bad name"),
    "simple identifier"
  )
})

test_that("render_secret_value renders logicals, numbers, and strings", {
  expect_equal(render_secret_value(TRUE), "true")
  expect_equal(render_secret_value(443), "443")
  expect_equal(render_secret_value("us-east-1"), "'us-east-1'")
  expect_equal(render_secret_value("o'clock"), "'o''clock'")
  expect_error(render_secret_value(c("a", "b")), "single non-missing")
})
