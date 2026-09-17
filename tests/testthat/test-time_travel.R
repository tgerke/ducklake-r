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

test_that("naive local POSIXct timestamps resolve as the correct instant", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, v = c("a", "b", "c")), "utc_asof")

  # Sys.time() carries no tzone attribute; before the UTC fix it was
  # rendered in local time but read by DuckLake as UTC, shifting the
  # queried instant by the UTC offset (hours in the past for tz < UTC,
  # "no snapshot found" here)
  asof_now <- get_ducklake_table_asof("utc_asof", Sys.time() + 5) |>
    dplyr::collect()
  expect_equal(nrow(asof_now), 3)

  # restore_table_version with a bare Sys.time() had the same skew
  get_ducklake_table("utc_asof") |>
    dplyr::filter(id != 3) |>
    replace_table("utc_asof")
  expect_true(restore_table_version("utc_asof", timestamp = Sys.time()))
  expect_equal(nrow(dplyr::collect(get_ducklake_table("utc_asof"))), 2)
})

test_that("time-travel readers are DuckLake tables that restore labels", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:3, v = c(1, 2, 3))
  attr(df$v, "label") <- "Value"
  suppressMessages(create_table(df, "tt_labels"))
  snapshots <- list_table_snapshots("tt_labels")

  by_version <- get_ducklake_table_version("tt_labels", snapshots$snapshot_id[[1]])
  expect_s3_class(by_version, "tbl_ducklake")
  expect_equal(attr(by_version, "ducklake_table_name"), "tt_labels")
  expect_equal(attr(dplyr::collect(by_version)$v, "label"), "Value")

  as_of <- get_ducklake_table_asof("tt_labels", snapshots$snapshot_time[[1]] + 1)
  expect_s3_class(as_of, "tbl_ducklake")
  expect_equal(attr(dplyr::collect(as_of)$v, "label"), "Value")
})

test_that("a versioned read restores the labels in force at that version", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:3, amount = c(1, 2, 3))
  attr(df$amount, "label") <- "amount v1"
  suppressMessages(create_table(df, "tt_asof_labels"))
  v1 <- max(list_table_snapshots("tt_asof_labels")$snapshot_id)
  suppressMessages(
    set_column_comments("tt_asof_labels", amount = "amount v2", id = "id added later")
  )
  v2 <- max(list_table_snapshots("tt_asof_labels")$snapshot_id)
  suppressMessages(set_column_comments("tt_asof_labels", amount = NA))

  old <- dplyr::collect(get_ducklake_table_version("tt_asof_labels", v1))
  expect_equal(attr(old$amount, "label"), "amount v1")
  expect_null(attr(old$id, "label"))

  # verbs keep the version
  mid <- get_ducklake_table_version("tt_asof_labels", v2) |>
    dplyr::filter(id > 1) |>
    dplyr::mutate(double = amount * 2) |>
    dplyr::collect()
  expect_equal(attr(mid$amount, "label"), "amount v2")
  expect_equal(attr(mid$id, "label"), "id added later")
  expect_null(attr(mid$double, "label"))

  # the live table is unaffected: the amount label was cleared
  now <- dplyr::collect(get_ducklake_table("tt_asof_labels"))
  expect_null(attr(now$amount, "label"))
  expect_equal(attr(now$id, "label"), "id added later")
})

test_that("an as-of read restores the labels in force at that time", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:3, amount = c(1, 2, 3))
  attr(df$amount, "label") <- "amount v1"
  suppressMessages(create_table(df, "tt_asof_time"))
  between <- Sys.time() + 1
  Sys.sleep(2)
  suppressMessages(set_column_comments("tt_asof_time", amount = "amount v2"))

  old <- dplyr::collect(get_ducklake_table_asof("tt_asof_time", between))
  expect_equal(attr(old$amount, "label"), "amount v1")

  # a snapshot's own time resolves to that snapshot, for the rows and the labels
  at_change <- max(list_table_snapshots("tt_asof_time")$snapshot_time)
  new <- dplyr::collect(get_ducklake_table_asof("tt_asof_time", at_change + 1))
  expect_equal(attr(new$amount, "label"), "amount v2")
})

test_that("versioned labels follow a column rename, a rebuild, and a table rename", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:3, amount = c(1, 2, 3))
  attr(df$amount, "label") <- "Amount as collected"
  suppressMessages(create_table(df, "tt_asof_moves"))
  v_start <- max(list_table_snapshots("tt_asof_moves")$snapshot_id)

  suppressMessages({
    rename_table_column("tt_asof_moves", "amount", "amt")
    set_column_comments("tt_asof_moves", amt = "Amount, renamed")
    get_ducklake_table("tt_asof_moves") |>
      dplyr::mutate(amt = amt * 2) |>
      replace_table("tt_asof_moves")
  })
  v_rebuilt <- max(list_table_snapshots("tt_asof_moves")$snapshot_id)
  suppressMessages({
    rename_ducklake_table("tt_asof_moves", "tt_asof_moved")
    set_column_comments("tt_asof_moved", amt = "Amount, after the move")
  })

  # DuckLake resolves the name as of the version, so the old name reads the old table
  start <- dplyr::collect(get_ducklake_table_version("tt_asof_moves", v_start))
  expect_named(start, c("id", "amount"))
  expect_equal(attr(start$amount, "label"), "Amount as collected")

  rebuilt <- dplyr::collect(get_ducklake_table_version("tt_asof_moves", v_rebuilt))
  expect_equal(attr(rebuilt$amt, "label"), "Amount, renamed")

  now <- dplyr::collect(get_ducklake_table("tt_asof_moved"))
  expect_equal(attr(now$amt, "label"), "Amount, after the move")
})

test_that("versioned labels work for a schema-qualified table and never fall back to today's", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:2, amount = c(1, 2))
  attr(df$amount, "label") <- "amount v1"
  suppressMessages({
    create_schema("bronze")
    create_table(df, "bronze.tt_asof_schema")
  })
  v1 <- max(list_table_snapshots("bronze.tt_asof_schema")$snapshot_id)
  suppressMessages(set_column_comments("bronze.tt_asof_schema", amount = "amount v2"))

  old <- get_ducklake_table_version("bronze.tt_asof_schema", v1)
  expect_equal(attr(dplyr::collect(old)$amount, "label"), "amount v1")

  # a lookup that fails leaves the snapshot's rows unlabelled
  testthat::local_mocked_bindings(
    column_comments_asof = function(...) stop("metadata unavailable")
  )
  unlabelled <- dplyr::collect(old)
  expect_equal(nrow(unlabelled), 2)
  expect_null(attr(unlabelled$amount, "label"))
})

test_that("list_table_snapshots matches id-only snapshots and survives a rewrite", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3), "snap_ids")
  # Row-level changes reference the table by id only
  suppressMessages(
    rows_insert(get_ducklake_table("snap_ids"), data.frame(id = 4L), by = "id")
  )
  # replace_table() gives the table a new id
  suppressMessages(
    get_ducklake_table("snap_ids") |>
      dplyr::filter(id > 1) |>
      replace_table("snap_ids")
  )
  create_table(data.frame(id = 1L), "snap_other")

  expect_equal(nrow(list_table_snapshots("snap_ids")), 3)
  expect_equal(nrow(list_table_snapshots("snap_other")), 1)
  expect_equal(nrow(list_table_snapshots("no_such_table")), 0)
})
