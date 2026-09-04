test_that("set_ducklake_retry() sets and reports the retry settings", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  on.exit(
    suppressMessages(set_ducklake_retry(max_retries = 10, wait_ms = 100, backoff = 1.5)),
    add = TRUE
  )

  expect_message(
    settings <- set_ducklake_retry(max_retries = 4, wait_ms = 50, backoff = 2),
    "up to 4 times"
  )
  expect_equal(settings$value[settings$name == "ducklake_max_retry_count"], "4")
  expect_equal(settings$value[settings$name == "ducklake_retry_wait_ms"], "50")
  expect_equal(as.numeric(settings$value[settings$name == "ducklake_retry_backoff"]), 2)

  suppressMessages(again <- set_ducklake_retry())
  expect_equal(again, settings)

  expect_error(set_ducklake_retry(max_retries = -1), "non-negative")
  expect_error(set_ducklake_retry(wait_ms = 1.5), "non-negative integer")
  expect_error(set_ducklake_retry(backoff = 0.5), "at least 1")
})
