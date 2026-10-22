# Catalog format versions: DUCKLAKE_VERSION on attach, AUTOMATIC_MIGRATION
# from 1.0 to 1.1, and the format version get_ducklake_info() reports

test_that("build_attach_sql renders DUCKLAKE_VERSION", {
  sql <- build_attach_sql(
    "lake", "/data", "duckdb", NULL,
    read_only = FALSE, ducklake_version = "1.0"
  )
  expect_match(sql, "DUCKLAKE_VERSION '1.0'", fixed = TRUE)

  sql <- build_attach_sql(
    "lake", "/data", "duckdb", NULL,
    read_only = FALSE, ducklake_version = "1.1", automatic_migration = TRUE
  )
  expect_match(sql, "DUCKLAKE_VERSION '1.1', AUTOMATIC_MIGRATION", fixed = TRUE)

  sql <- build_attach_sql("lake", "/data", "duckdb", NULL, read_only = FALSE)
  expect_no_match(sql, "DUCKLAKE_VERSION")
})

test_that("build_attach_sql accepts only the strings 1.0 and 1.1", {
  for (bad in list("1.2", "0.4", 1.1, c("1.0", "1.1"), NA_character_)) {
    expect_error(
      build_attach_sql(
        "lake", "/data", "duckdb", NULL,
        read_only = FALSE, ducklake_version = bad
      ),
      "must be the string"
    )
  }
})

test_that("a lake created at version 1.0 migrates to 1.1 once, and never back", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake(ducklake_version = "1.0")
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  expect_equal(get_ducklake_info()$format_version, "1.0")

  create_table(mtcars, "cars")
  snapshots_before <- nrow(list_table_snapshots())

  # Requesting 1.1 without migration is DuckLake's error, not a new lake
  detach_ducklake(lake$ducklake_name)
  expect_error(
    attach_ducklake(
      lake$ducklake_name, lake_path = lake$lake_path, ducklake_version = "1.1"
    ),
    "AUTOMATIC_MIGRATION"
  )

  # Without a version the lake opens in the format it has
  attach_ducklake(lake$ducklake_name, lake_path = lake$lake_path)
  expect_equal(get_ducklake_info()$format_version, "1.0")

  # Migration runs on the attach, keeps the data and the history, and
  # writes no snapshot of its own
  detach_ducklake(lake$ducklake_name)
  attach_ducklake(
    lake$ducklake_name, lake_path = lake$lake_path, automatic_migration = TRUE
  )
  expect_equal(get_ducklake_info()$format_version, "1.1")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("cars"))), 32)
  expect_equal(nrow(list_table_snapshots()), snapshots_before)

  # Later attaches need nothing, and a downgrade is refused
  detach_ducklake(lake$ducklake_name)
  attach_ducklake(lake$ducklake_name, lake_path = lake$lake_path)
  expect_equal(get_ducklake_info()$format_version, "1.1")
  detach_ducklake(lake$ducklake_name)
  expect_error(
    attach_ducklake(
      lake$ducklake_name, lake_path = lake$lake_path, ducklake_version = "1.0"
    ),
    "downgrade"
  )
})

test_that("a SQLite catalog takes a version and migrates the same way", {
  skip_if_no_ducklake()
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  temp_dir <- tempfile("test_sqlite_version")
  dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)
  sqlite_catalog <- file.path(temp_dir, "metadata.sqlite")
  sqlite_data <- file.path(temp_dir, "data")
  dir.create(sqlite_data, showWarnings = FALSE, recursive = TRUE)

  tryCatch(
    {
      attach_ducklake(
        "sqlite_version_lake",
        backend = "sqlite",
        catalog_connection_string = sqlite_catalog,
        lake_path = sqlite_data,
        ducklake_version = "1.0"
      )
      expect_equal(get_ducklake_info()$format_version, "1.0")
      create_table(mtcars, "cars")

      detach_ducklake("sqlite_version_lake")
      attach_ducklake(
        "sqlite_version_lake",
        backend = "sqlite",
        catalog_connection_string = sqlite_catalog,
        lake_path = sqlite_data,
        automatic_migration = TRUE
      )
      expect_equal(get_ducklake_info()$format_version, "1.1")
      expect_equal(nrow(dplyr::collect(get_ducklake_table("cars"))), 32)

      detach_ducklake("sqlite_version_lake", shutdown = TRUE)
    },
    finally = {
      unlink(temp_dir, recursive = TRUE)
    }
  )
})
