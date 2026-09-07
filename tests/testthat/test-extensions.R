# Tests for the on-demand extension install path and its hints

test_that("load_or_install_extension installs after a failed LOAD and says so", {
  env <- get_ducklake_env()
  old <- env$ext_available
  on.exit(env$ext_available <- old, add = TRUE)
  env$ext_available <- FALSE

  executed <- character()
  local_mocked_bindings(
    db_execute = function(sql, ...) {
      executed <<- c(executed, sql)
      if (sql == "LOAD ducklake;" && !"INSTALL ducklake;" %in% executed) {
        stop("Extension \"ducklake.duckdb_extension\" not found.")
      }
      invisible(NULL)
    },
    extension_directory = function(conn) file.path(tempdir(), "extensions")
  )

  expect_message(
    load_or_install_extension("ducklake", conn = NULL),
    "Installing the ducklake DuckDB extension"
  )
  expect_equal(
    executed,
    c("LOAD ducklake;", "INSTALL ducklake;", "LOAD ducklake;")
  )
  # a first-use install invalidates a cached FALSE from the probe
  expect_null(env$ext_available)
})

test_that("load_or_install_extension only loads an installed extension", {
  executed <- character()
  local_mocked_bindings(
    db_execute = function(sql, ...) {
      executed <<- c(executed, sql)
      invisible(NULL)
    }
  )

  expect_silent(load_or_install_extension("httpfs", conn = NULL))
  expect_equal(executed, "LOAD httpfs;")
})

test_that("extension_persistence_hint flags only a temporary directory", {
  expect_length(extension_persistence_hint(NA_character_), 0)
  expect_length(extension_persistence_hint(character()), 0)

  durable <- file.path(dirname(tempdir()), "not-this-session", "extensions")
  expect_length(extension_persistence_hint(durable), 0)

  hint <- extension_persistence_hint(file.path(tempdir(), "extensions"))
  expect_named(hint, c("!", "i"))
  expect_match(hint[["!"]], "temporary")
  expect_match(hint[["i"]], "DUCKDB_R_HOME")
})

test_that("storage_home_root is the parent of duckdb's extension directory", {
  status <- data.frame(
    kind = c("extensions", "stored_secrets"),
    source = "shared",
    directory = c("/home/me/.duckdb/extensions", "/home/me/.duckdb/stored_secrets")
  )
  expect_equal(storage_home_root(status), "/home/me/.duckdb")
  expect_error(storage_home_root(status[2, ]), "extension directory")
})
