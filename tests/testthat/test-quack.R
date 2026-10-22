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

# --- Input validation (runs before the engine is touched) ---

test_that("attach_quack requires a uri", {
  expect_snapshot(error = TRUE, attach_quack("team"))
})

test_that("quack_query requires a single query string", {
  expect_snapshot(error = TRUE, quack_query("quack:localhost", c("a", "b")))
})

# --- Live round trip ---

test_that("quack_serve and quack_query complete a live round trip", {
  skip_on_cran()
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

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

test_that("install_quack installs and optionally loads the extension", {
  executed <- character()
  local_mocked_bindings(
    db_execute = function(sql, ...) {
      executed <<- c(executed, sql)
      invisible(NULL)
    }
  )

  suppressMessages(install_quack())
  expect_equal(executed, c("INSTALL quack;", "LOAD quack;"))

  executed <- character()
  suppressMessages(install_quack(load = FALSE))
  expect_equal(executed, "INSTALL quack;")
})

test_that("detach_quack removes the catalog and ignores unknown names", {
  skip_if_not_installed("duckdb")

  conn <- get_ducklake_connection()
  DBI::dbExecute(conn, "ATTACH ':memory:' AS quack_detach_test;")

  detach_quack("quack_detach_test")

  attached <- DBI::dbGetQuery(
    conn,
    "SELECT database_name FROM duckdb_databases();"
  )$database_name
  expect_false("quack_detach_test" %in% attached)

  # Unknown names and NULL are silent no-ops
  expect_no_error(detach_quack("no_such_catalog"))
  expect_no_error(detach_quack())
})

# --- DuckDB 2.0 options ---

test_that("build_quack_attach_sql renders the client options", {
  expect_equal(
    ducklake:::build_quack_attach_sql(
      "team", "quack:localhost", token = "s3cret",
      ssl_fingerprint = strrep("ab", 32), client_id = "ducklake_r",
      heartbeat_timeout = 60
    ),
    sprintf(
      "ATTACH 'quack:localhost' AS team (TYPE quack, TOKEN 's3cret', SSL_FINGERPRINT '%s', CLIENT_ID 'ducklake_r', HEARTBEAT_TIMEOUT 60);",
      strrep("ab", 32)
    )
  )
  expect_error(
    ducklake:::build_quack_attach_sql("team", "quack:localhost", heartbeat_timeout = -1),
    "positive number"
  )
})

test_that("build_quack_query_sql renders the client options as named arguments", {
  expect_equal(
    ducklake:::build_quack_query_sql(
      "localhost", "SELECT 1", token = "s3cret", client_id = "ducklake_r",
      heartbeat_timeout = 30
    ),
    "SELECT * FROM quack_query('quack:localhost', 'SELECT 1', token = 's3cret', client_id = 'ducklake_r', heartbeat_timeout = 30);"
  )
})

test_that("build_quack_serve_sql renders the server options", {
  expect_equal(
    ducklake:::build_quack_serve_sql("quack:localhost", token = "s3cret"),
    "CALL quack_serve('quack:localhost', token = 's3cret');"
  )
  expect_equal(
    ducklake:::build_quack_serve_sql(
      "quack:0.0.0.0:9494", secret = "team_token", allow_other_hostname = TRUE,
      ssl_cert_file = "/keys/server.pem", ssl_key_file = "/keys/private_key.pem"
    ),
    "CALL quack_serve('quack:0.0.0.0:9494', secret = 'team_token', allow_other_hostname = true, ssl_cert_file = '/keys/server.pem', ssl_key_file = '/keys/private_key.pem');"
  )
  expect_equal(
    ducklake:::build_quack_serve_sql("localhost", create_secret_if_not_exists = TRUE),
    "CALL quack_serve('quack:localhost', create_secret_if_not_exists = true);"
  )
  expect_error(
    ducklake:::build_quack_serve_sql("localhost", token = "a", secret = "b"),
    "not both"
  )
  expect_error(
    ducklake:::build_quack_serve_sql("localhost", ssl_cert_file = "/keys/server.pem"),
    "go together"
  )
})

test_that("build_attach_sql renders a Quack catalog with META_TOKEN", {
  sql <- build_attach_sql(
    "lake", "/shared/data", "quack", "catalog.example.org:9494",
    read_only = FALSE, quack_token = "s3cret"
  )
  expect_match(sql, "ATTACH 'ducklake:quack:catalog.example.org:9494' AS lake", fixed = TRUE)
  expect_match(sql, "META_TOKEN 's3cret'", fixed = TRUE)
  expect_error(
    attach_ducklake("lake", lake_path = tempdir(), quack_token = "s3cret"),
    "quack.*backend only"
  )
})

test_that("a DuckLake catalog served over Quack works through attach_ducklake", {
  skip_on_cran()
  skip_on_os("windows")
  skip_if_not_installed("duckdb")
  skip_if_no_ducklake()

  dir <- tempfile("quack_catalog")
  dir.create(dir)
  data <- file.path(dir, "data")
  dir.create(data)
  port <- 19500 + sample.int(400, 1)
  stop_file <- file.path(dir, "stop")
  on.exit(writeLines("stop", stop_file), add = TRUE)

  # The server is a second R process holding the catalog file open: a
  # DuckDB-file catalog admits one process, and this is how it is shared
  server <- file.path(dir, "serve.R")
  writeLines(c(
    "args <- commandArgs(TRUE)",
    "dir <- args[1]; port <- args[2]",
    "con <- DBI::dbConnect(duckdb::duckdb(), dbdir = file.path(dir, 'catalog.duckdb'))",
    "DBI::dbExecute(con, 'INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs; INSTALL quack; LOAD quack')",
    "DBI::dbGetQuery(con, sprintf(\"CALL quack_serve('quack:localhost:%s', token = 'test_secret')\", port))",
    "writeLines('ready', file.path(dir, 'ready'))",
    "deadline <- Sys.time() + 180",
    "while (!file.exists(file.path(dir, 'stop')) && Sys.time() < deadline) Sys.sleep(0.2)",
    "DBI::dbDisconnect(con, shutdown = TRUE)"
  ), server)
  system2(
    file.path(R.home("bin"), "Rscript"), c(shQuote(server), shQuote(dir), port),
    wait = FALSE, stdout = file.path(dir, "server.log"), stderr = file.path(dir, "server.log")
  )
  ready <- FALSE
  for (i in seq_len(150)) {
    if (file.exists(file.path(dir, "ready"))) {
      ready <- TRUE
      break
    }
    Sys.sleep(0.2)
  }
  skip_if_not(ready, "Could not start a Quack server process")

  suppressMessages(attach_ducklake(
    "quack_lake", lake_path = data, backend = "quack",
    catalog_connection_string = sprintf("localhost:%d", port),
    quack_token = "test_secret", author = "Tester"
  ))
  on.exit(try(detach_ducklake("quack_lake"), silent = TRUE), add = TRUE, after = FALSE)
  expect_equal(get_ducklake_backend("quack_lake"), "quack")

  suppressMessages({
    create_table(data.frame(id = 1:3, grp = c("a", "b", "a")), "served")
    rows_insert(get_ducklake_table("served"), data.frame(id = 4L, grp = "b"), by = "id")
    set_table_comment("served", "over quack")
  })
  expect_equal(nrow(dplyr::collect(get_ducklake_table("served"))), 4)
  expect_equal(get_table_comments("served")$comment, "over quack")
  snaps <- list_table_snapshots("served")
  expect_equal(nrow(snaps), 3)
  # the creation snapshot was labeled through the served catalog
  all_snaps <- list_table_snapshots()
  expect_equal(all_snaps$author[all_snaps$snapshot_id == 0], "Tester")
  expect_equal(nrow(dplyr::collect(get_ducklake_table_version("served", snaps$snapshot_id[[1]]))), 3)
  expect_equal(get_ducklake_info("quack_lake")$backend, "quack")

  expect_warning(
    suppressMessages(backup_ducklake("quack_lake", data, file.path(dir, "backup"))),
    "Quack server"
  )
})
