# DUCKLAKE FEATURE TESTS: Time Travel
#
# These tests verify DuckLake-specific time travel functionality.
# Time travel queries are a core ducklake feature.

test_that("get_ducklake_table_asof formats POSIXct timestamps", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  # Create a real ducklake
  lake <- create_temp_ducklake()

  # Create a table with some data
  test_data <- data.frame(id = 1:3, value = c("a", "b", "c"), stringsAsFactors = FALSE)
  create_table(test_data, "asof_test1")

  # Get the snapshot time
  snapshots <- list_table_snapshots("asof_test1")
  expect_gt(nrow(snapshots), 0)

  # Query at a time after the snapshot - this should reliably find the snapshot
  snapshot_time <- snapshots$snapshot_time[nrow(snapshots)]
  query_time <- snapshot_time + 1  # Query 1 second after the snapshot time

  # Query at that timestamp - should get the snapshot
  result <- get_ducklake_table_asof("asof_test1", query_time)

  # Should be able to collect the data
  collected <- dplyr::collect(result)
  expect_equal(nrow(collected), 3)

  cleanup_temp_ducklake(lake)
})

test_that("get_ducklake_table_asof returns lazy table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  # Create a real ducklake
  lake <- create_temp_ducklake()

  # Create a table
  test_data <- data.frame(id = 1:5, name = letters[1:5], stringsAsFactors = FALSE)
  create_table(test_data, "asof_test3")

  # Get snapshot and query after it to ensure it's available
  snapshots <- list_table_snapshots("asof_test3")
  ts <- snapshots$snapshot_time[nrow(snapshots)] + 1

  # Query at timestamp
  result <- get_ducklake_table_asof("asof_test3", ts)

  # Verify it's a lazy table
  expect_s3_class(result, "tbl_lazy")

  # Verify we can use dplyr verbs on it
  filtered <- result |> dplyr::filter(id > 2)
  expect_s3_class(filtered, "tbl_lazy")

  # Verify we can collect it
  collected <- dplyr::collect(filtered)
  expect_equal(nrow(collected), 3)
  expect_true(all(collected$id > 2))

  cleanup_temp_ducklake(lake)
})

test_that("get_ducklake_table_version queries specific snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  # Create a real ducklake
  lake <- create_temp_ducklake()

  # Create initial table (version 1)
  test_data_v1 <- data.frame(id = 1:3, status = "original", stringsAsFactors = FALSE)
  create_table(test_data_v1, "version_test")

  # Get version 1 snapshot ID
  snapshots_v1 <- list_table_snapshots("version_test")
  version_1 <- snapshots_v1$snapshot_id[nrow(snapshots_v1)]

  # Modify the table (version 2)
  with_transaction({
    get_ducklake_table("version_test") |>
      dplyr::mutate(status = "modified") |>
      replace_table("version_test")
  })

  # Verify current table has modified data
  current <- get_ducklake_table("version_test") |> dplyr::collect()
  expect_equal(current$status, rep("modified", 3))

  # Query version 1 - should get ORIGINAL data, not modified
  v1_result <- get_ducklake_table_version("version_test", version_1) |> dplyr::collect()
  expect_equal(nrow(v1_result), 3)
  expect_equal(v1_result$status, rep("original", 3))

  cleanup_temp_ducklake(lake)
})

test_that("time travel functions use default connection", {
  skip_if_not_installed("duckdb")

  # Create a real ducklake
  lake <- create_temp_ducklake()

  # Create table
  test_data <- data.frame(id = 1:3, val = c("a", "b", "c"), stringsAsFactors = FALSE)
  create_table(test_data, "default_conn_test")

  # Get snapshot info
  snapshots <- list_table_snapshots("default_conn_test")
  version <- snapshots$snapshot_id[nrow(snapshots)]

  # Functions should work without explicit conn parameter
  result2 <- get_ducklake_table_version("default_conn_test", version)
  expect_s3_class(result2, "tbl_lazy")

  cleanup_temp_ducklake(lake)
})

test_that("time travel functions work with explicit connection", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  
  # Create table
  test_data <- data.frame(id = 1:3, val = c("a", "b", "c"), stringsAsFactors = FALSE)
  create_table(test_data, "explicit_conn_test")
  
  # Get snapshot info
  snapshots <- list_table_snapshots("explicit_conn_test")
  version <- snapshots$snapshot_id[nrow(snapshots)]
  
  # Get the connection explicitly
  conn <- get_ducklake_connection()
  
  # Functions should work with explicit conn parameter
  result <- get_ducklake_table_version("explicit_conn_test", version, conn = conn)
  expect_s3_class(result, "tbl_lazy")
  
  cleanup_temp_ducklake(lake)
})
