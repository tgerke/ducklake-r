# Quack needs a live DuckDB >= 1.5.3 server, so these tests cover URI handling,
# SQL generation, and the version gate. End-to-end serve/attach is not exercised
# (no infra-free path), the same approach as test-multi-backend.R.

# --- URI normalization ---

test_that("build_quack_uri prepends the scheme only when missing", {
  expect_equal(ducklake:::build_quack_uri("localhost"), "quack:localhost")
  expect_equal(ducklake:::build_quack_uri("quack:localhost"), "quack:localhost")
  expect_equal(ducklake:::build_quack_uri("quack://host:9000"), "quack://host:9000")
  expect_equal(
    ducklake:::build_quack_uri("data.example.org:9494"),
    "quack:data.example.org:9494"
  )
})

test_that("build_quack_uri rejects invalid input", {
  expect_snapshot(error = TRUE, ducklake:::build_quack_uri(""))
  expect_snapshot(error = TRUE, ducklake:::build_quack_uri(c("a", "b")))
  expect_snapshot(error = TRUE, ducklake:::build_quack_uri(NA_character_))
})

# --- ATTACH SQL generation ---

test_that("build_quack_attach_sql uses TYPE quack by default", {
  expect_equal(
    ducklake:::build_quack_attach_sql("team", "quack:localhost"),
    "ATTACH 'quack:localhost' AS team (TYPE quack);"
  )
})

test_that("build_quack_attach_sql adds a TOKEN when supplied", {
  expect_equal(
    ducklake:::build_quack_attach_sql("team", "quack:localhost", token = "super_secret"),
    "ATTACH 'quack:localhost' AS team (TYPE quack, TOKEN 'super_secret');"
  )
})

test_that("build_quack_attach_sql adds DISABLE_SSL when requested", {
  expect_equal(
    ducklake:::build_quack_attach_sql(
      "team",
      "quack:localhost",
      token = "x",
      disable_ssl = TRUE
    ),
    "ATTACH 'quack:localhost' AS team (TYPE quack, TOKEN 'x', DISABLE_SSL);"
  )
})

test_that("build_quack_attach_sql prefixes a bare host", {
  expect_equal(
    ducklake:::build_quack_attach_sql("team", "localhost"),
    "ATTACH 'quack:localhost' AS team (TYPE quack);"
  )
})

# --- SQL literal quoting ---

test_that("quote_sql escapes embedded single quotes", {
  expect_equal(ducklake:::quote_sql("plain"), "'plain'")
  expect_equal(ducklake:::quote_sql("O'Brien"), "'O''Brien'")
})

# --- Version gate ---

test_that("quack_version_supported compares against 1.5.3", {
  expect_equal(ducklake:::quack_version_supported("v1.5.2"), FALSE)
  expect_equal(ducklake:::quack_version_supported("v1.5.3"), TRUE)
  expect_equal(ducklake:::quack_version_supported("1.6.0"), TRUE)
  expect_equal(ducklake:::quack_version_supported("v1.10.0"), TRUE)
  expect_equal(ducklake:::quack_version_supported("not-a-version"), FALSE)
})

# --- Input validation (runs before the engine is touched) ---

test_that("attach_quack requires a uri", {
  expect_snapshot(error = TRUE, attach_quack("team"))
})

test_that("quack_query requires a single query string", {
  expect_snapshot(error = TRUE, quack_query("quack:localhost", c("a", "b")))
})

# --- Live round trip (needs a DuckDB >= 1.5.3 engine; skipped otherwise) ---

test_that("quack_serve and quack_query complete a live round trip", {
  skip_on_cran()
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  engine <- DBI::dbGetQuery(get_ducklake_connection(), "SELECT version() AS v")$v
  skip_if_not(
    ducklake:::quack_version_supported(engine),
    "DuckDB engine is older than 1.5.3"
  )

  temp_dir <- tempfile("quack_live")
  dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)
  uri <- "quack:localhost:19494"
  token <- "test_secret"

  served <- tryCatch(
    {
      attach_ducklake("quack_live_lake", lake_path = temp_dir)
      with_transaction(
        create_table(data.frame(id = 1:3, grp = c("a", "a", "b")), "t")
      )
      quack_serve(uri, token = token)
      TRUE
    },
    error = function(e) FALSE
  )
  skip_if_not(served, "Could not start a local Quack server")

  tryCatch(
    {
      n <- quack_query(
        uri,
        "SELECT count(*) AS n FROM quack_live_lake.t",
        token = token
      )
      expect_equal(as.numeric(n$n), 3)

      filtered <- quack_query(
        uri,
        "SELECT id FROM quack_live_lake.t WHERE grp = 'b'",
        token = token
      )
      expect_equal(as.numeric(filtered$id), 3)
    },
    finally = {
      try(quack_stop(uri), silent = TRUE)
      try(detach_ducklake("quack_live_lake", shutdown = TRUE), silent = TRUE)
      unlink(temp_dir, recursive = TRUE)
    }
  )
})
