# Tests for add_data_files() and list_ducklake_files()

test_that("add_data_files registers an existing parquet file without copying", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:3, v = c("a", "b", "c")), "adf_target")
  files_before <- list_ducklake_files("adf_target")

  # A parquet file written outside the lake, with a matching schema
  external_file <- file.path(lake$temp_dir, "external.parquet")
  DBI::dbExecute(
    lake$conn,
    sprintf(
      "COPY (SELECT 4 AS id, 'd' AS v) TO '%s' (FORMAT parquet);",
      external_file
    )
  )

  suppressMessages(add_data_files("adf_target", external_file))

  result <- dplyr::collect(get_ducklake_table("adf_target"))
  expect_equal(nrow(result), 4)
  expect_setequal(result$id, 1:4)

  files_after <- list_ducklake_files("adf_target")
  expect_equal(nrow(files_after), nrow(files_before) + 1)
  expect_true(any(grepl("external.parquet", files_after$data_file, fixed = TRUE)))

  cleanup_temp_ducklake(lake)
})

test_that("list_ducklake_files respects snapshot_version", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:3), "ldf_history")
  v1 <- max(list_table_snapshots("ldf_history")$snapshot_id)

  external_file <- file.path(lake$temp_dir, "later.parquet")
  DBI::dbExecute(
    lake$conn,
    sprintf("COPY (SELECT 4 AS id) TO '%s' (FORMAT parquet);", external_file)
  )
  suppressMessages(add_data_files("ldf_history", external_file))

  now_files <- list_ducklake_files("ldf_history")
  then_files <- list_ducklake_files("ldf_history", snapshot_version = v1)
  expect_equal(nrow(now_files), nrow(then_files) + 1)

  cleanup_temp_ducklake(lake)
})

test_that("add_data_files and list_ducklake_files validate their inputs", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()

  expect_error(
    add_data_files("t", character(0)),
    "character vector of file paths"
  )
  expect_error(
    list_ducklake_files(
      "t",
      snapshot_version = 1,
      snapshot_time = "2026-01-01 00:00:00"
    ),
    "only one of"
  )

  cleanup_temp_ducklake(lake)
})
