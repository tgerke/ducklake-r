# Multi-backend catalog support
# Only DuckDB and SQLite backends are tested since they need no external infra.

# --- Input validation ---

test_that("attach_ducklake requires catalog_connection_string for non-DuckDB backends", {
  skip_if_not_installed("duckdb")

  expect_error(
    attach_ducklake("test", lake_path = "/tmp", backend = "postgres"),
    "catalog_connection_string"
  )
  expect_error(
    attach_ducklake("test", lake_path = "/tmp", backend = "sqlite"),
    "catalog_connection_string"
  )
  expect_error(
    attach_ducklake("test", lake_path = "/tmp", backend = "mysql"),
    "catalog_connection_string"
  )
})

test_that("attach_ducklake requires lake_path for all backends", {
  skip_if_not_installed("duckdb")

  # DuckDB backend
  expect_error(
    attach_ducklake("test"),
    "lake_path"
  )
  expect_error(
    attach_ducklake("test", lake_path = NULL),
    "lake_path"
  )

  # Non-DuckDB backends
  expect_error(
    attach_ducklake(
      "test",
      backend = "sqlite",
      catalog_connection_string = "test.sqlite"
    ),
    "lake_path"
  )
  expect_error(
    attach_ducklake(
      "test",
      backend = "postgres",
      catalog_connection_string = "dbname=test"
    ),
    "lake_path"
  )
})

test_that("attach_ducklake rejects invalid backend values", {
  skip_if_not_installed("duckdb")

  expect_error(
    attach_ducklake("test", lake_path = "/tmp", backend = "oracle")
  )
})

# --- Backend metadata ---

test_that("get_ducklake_backend returns 'duckdb' by default", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  expect_equal(get_ducklake_backend(), "duckdb")

  cleanup_temp_ducklake(lake)
})

# --- SQL generation ---

test_that("build_attach_sql generates correct SQL for DuckDB backend", {
  # With lake_path
  sql <- ducklake:::build_attach_sql("my_lake", "/data", "duckdb", NULL, FALSE)
  expect_true(grepl("ducklake:", sql))
  expect_true(grepl("DATA_PATH '/data'", sql, fixed = TRUE))
  expect_false(grepl("READ_ONLY", sql))

  # With read_only
  sql <- ducklake:::build_attach_sql("my_lake", "/data", "duckdb", NULL, TRUE)
  expect_true(grepl("READ_ONLY", sql))
  expect_true(grepl("DATA_PATH '/data'", sql, fixed = TRUE))
})

test_that("build_attach_sql honors catalog_connection_string for DuckDB backend", {
  sql <- ducklake:::build_attach_sql(
    "my_lake",
    "s3://bucket/data",
    "duckdb",
    "/lakes/my_lake.ducklake",
    FALSE
  )
  expect_true(grepl("ATTACH 'ducklake:/lakes/my_lake.ducklake'", sql, fixed = TRUE))
  expect_true(grepl("DATA_PATH 's3://bucket/data'", sql, fixed = TRUE))
  expect_false(grepl("s3://bucket/data/my_lake.ducklake", sql, fixed = TRUE))

  # NULL still derives the catalog path from lake_path
  sql2 <- ducklake:::build_attach_sql("my_lake", "/data", "duckdb", NULL, FALSE)
  expect_true(grepl("ducklake:/data/my_lake.ducklake", sql2, fixed = TRUE))
})

test_that("attach_ducklake refuses to write a duckdb catalog to object storage", {
  skip_if_not_installed("duckdb")

  expect_error(
    attach_ducklake("remote_lake", lake_path = "s3://bucket/data"),
    "catalog_connection_string"
  )
  expect_error(
    attach_ducklake(
      "remote_lake",
      lake_path = "s3://bucket/data",
      catalog_connection_string = "s3://bucket/remote_lake.ducklake"
    ),
    "object storage"
  )

  # read_only leaves direct remote attach available
  sql <- ducklake:::build_attach_sql(
    "remote_lake",
    "s3://bucket/data",
    "duckdb",
    "s3://bucket/remote_lake.ducklake",
    TRUE
  )
  expect_true(grepl("ducklake:s3://bucket/remote_lake.ducklake", sql, fixed = TRUE))
  expect_true(grepl("READ_ONLY", sql))
})

test_that("build_attach_sql generates correct SQL for PostgreSQL backend", {
  sql <- ducklake:::build_attach_sql(
    "my_lake",
    "/data",
    "postgres",
    "dbname=catalog host=localhost",
    FALSE
  )
  expect_true(grepl(
    "ducklake:postgres:dbname=catalog host=localhost",
    sql,
    fixed = TRUE
  ))
  expect_true(grepl("DATA_PATH '/data'", sql, fixed = TRUE))
})

test_that("build_attach_sql generates correct SQL for SQLite backend", {
  sql <- ducklake:::build_attach_sql(
    "my_lake",
    "/data",
    "sqlite",
    "metadata.sqlite",
    FALSE
  )
  expect_true(grepl("ducklake:sqlite:metadata.sqlite", sql, fixed = TRUE))
  expect_true(grepl("DATA_PATH '/data'", sql, fixed = TRUE))
})

test_that("build_attach_sql generates correct SQL for MySQL backend", {
  sql <- ducklake:::build_attach_sql(
    "my_lake",
    "/data",
    "mysql",
    "db=catalog host=localhost",
    FALSE
  )
  expect_true(grepl(
    "ducklake:mysql:db=catalog host=localhost",
    sql,
    fixed = TRUE
  ))
  expect_true(grepl("DATA_PATH '/data'", sql, fixed = TRUE))
})

test_that("build_attach_sql combines DATA_PATH and READ_ONLY options", {
  sql <- ducklake:::build_attach_sql(
    "my_lake",
    "/data",
    "postgres",
    "dbname=catalog",
    TRUE
  )
  expect_true(grepl("DATA_PATH", sql))
  expect_true(grepl("READ_ONLY", sql))
})

test_that("build_attach_sql includes OVERRIDE_DATA_PATH when requested", {
  sql <- ducklake:::build_attach_sql(
    "my_lake",
    "/backup/data",
    "duckdb",
    NULL,
    FALSE,
    override_data_path = TRUE
  )
  expect_true(grepl("OVERRIDE_DATA_PATH TRUE", sql, fixed = TRUE))
  expect_true(grepl("DATA_PATH", sql))

  # Not included by default
  sql2 <- ducklake:::build_attach_sql("my_lake", "/data", "duckdb", NULL, FALSE)
  expect_false(grepl("OVERRIDE_DATA_PATH", sql2))
})

# --- SQLite backend end-to-end ---

test_that("SQLite backend: create table, query, and time travel", {
  skip_if_no_ducklake()
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  temp_dir <- tempfile("test_sqlite")
  dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)
  sqlite_catalog <- file.path(temp_dir, "metadata.sqlite")
  sqlite_data <- file.path(temp_dir, "data")
  dir.create(sqlite_data, showWarnings = FALSE, recursive = TRUE)

  tryCatch(
    {
      attach_ducklake(
        "test_sqlite_backend",
        backend = "sqlite",
        catalog_connection_string = sqlite_catalog,
        lake_path = sqlite_data
      )

      expect_equal(get_ducklake_backend(), "sqlite")

      # Create a table
      with_transaction(
        create_table(mtcars, "cars"),
        author = "Test",
        commit_message = "Initial load"
      )

      result <- get_ducklake_table("cars") |> dplyr::collect()
      expect_equal(nrow(result), 32)
      expect_true("mpg" %in% names(result))

      expect_true(file.exists(sqlite_catalog))

      snapshots <- list_table_snapshots("cars")
      expect_equal(nrow(snapshots), 1)

      # Replace table and check second snapshot
      with_transaction(
        {
          get_ducklake_table("cars") |>
            dplyr::mutate(kpl = mpg * 0.425144) |>
            replace_table("cars")
        },
        author = "Test",
        commit_message = "Add kpl column"
      )

      result2 <- get_ducklake_table("cars") |> dplyr::collect()
      expect_true("kpl" %in% names(result2))

      snapshots2 <- list_table_snapshots("cars")
      expect_equal(nrow(snapshots2), 2)

      # Time travel: version 1 should not have kpl
      v1 <- get_ducklake_table_version("cars", snapshots2$snapshot_id[1]) |>
        dplyr::collect()
      expect_false("kpl" %in% names(v1))

      # Full shutdown to release all resources
      detach_ducklake("test_sqlite_backend", shutdown = TRUE)
    },
    finally = {
      unlink(temp_dir, recursive = TRUE)
    }
  )
})

# --- DuckDB backend with a split catalog/data layout ---

test_that("duckdb backend: local catalog with separate data path round-trips", {
  skip_if_no_ducklake()
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  catalog_dir <- tempfile("test_split_catalog")
  data_dir <- tempfile("test_split_data")
  backup_root <- tempfile("test_split_backup")
  for (d in c(catalog_dir, data_dir, backup_root)) {
    dir.create(d, showWarnings = FALSE, recursive = TRUE)
  }
  catalog_file <- file.path(catalog_dir, "split.ducklake")

  tryCatch(
    {
      attach_ducklake(
        "split_lake",
        lake_path = data_dir,
        catalog_connection_string = catalog_file
      )

      create_table(mtcars, "cars")

      expect_true(file.exists(catalog_file))
      parquet_files <- list.files(
        data_dir,
        pattern = "\\.parquet$",
        recursive = TRUE
      )
      expect_gt(length(parquet_files), 0)

      # Survives a full shutdown and re-attach with the same layout
      detach_ducklake("split_lake", shutdown = TRUE)
      attach_ducklake(
        "split_lake",
        lake_path = data_dir,
        catalog_connection_string = catalog_file
      )
      result <- get_ducklake_table("cars") |> dplyr::collect()
      expect_equal(nrow(result), 32)

      # backup_ducklake() finds the catalog outside lake_path via the registry
      backup_dir <- suppressMessages(
        backup_ducklake("split_lake", lake_path = data_dir, backup_path = backup_root)
      )
      expect_true(file.exists(file.path(backup_dir, "split.ducklake")))

      detach_ducklake("split_lake", shutdown = TRUE)
    },
    finally = {
      unlink(c(catalog_dir, data_dir, backup_root), recursive = TRUE)
    }
  )
})
