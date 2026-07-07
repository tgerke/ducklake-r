# UNIT TESTS: Data inlining operations
#
# These tests verify the data inlining feature: configuring inlining thresholds,
# verifying that small writes are inlined (no Parquet files created), flushing
# inlined data to Parquet, and the checkpoint_ducklake() helper.
#
# Tests use with_transaction() + create_table() / replace_table() where a
# full-table operation suffices. Tests that need to exercise *incremental*
# writes to an existing table use rows_insert() / rows_delete(), since those
# are the exported functions that trigger true per-row inlining.

test_that("attach_ducklake accepts data_inlining_row_limit parameter", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  temp_dir <- tempfile()
  dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)
  ducklake_name <- paste0(
    "test_inline_attach_",
    format(Sys.time(), "%Y%m%d%H%M%S")
  )

  # Build SQL with inlining limit and verify it appears in the statement
  sql <- build_attach_sql(
    ducklake_name,
    temp_dir,
    "duckdb",
    catalog_connection_string = NULL,
    read_only = FALSE,
    override_data_path = FALSE,
    data_inlining_row_limit = 50
  )
  expect_match(sql, "DATA_INLINING_ROW_LIMIT 50")

  # Build SQL without inlining limit and verify it does NOT appear

  sql_no_limit <- build_attach_sql(
    ducklake_name,
    temp_dir,
    "duckdb",
    catalog_connection_string = NULL,
    read_only = FALSE,
    override_data_path = FALSE
  )
  expect_no_match(sql_no_limit, "DATA_INLINING_ROW_LIMIT")

  unlink(temp_dir, recursive = TRUE)
})

test_that("small inserts are inlined (no Parquet files created)", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Create a small table (3 rows, below default threshold of 10)
  with_transaction(
    create_table(
      data.frame(sensor_id = 1:2, temperature = c(21.5, 22.1)),
      "readings"
    ),
    author = "test",
    commit_message = "create readings table"
  )

  # Verify data is queryable
  result <- get_ducklake_table("readings") |> dplyr::collect()
  expect_equal(nrow(result), 2)

  # Check that no Parquet data files were created (data is inlined)
  conn <- get_ducklake_connection()
  file_count <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'readings');",
      lake$ducklake_name
    )
  )$n
  expect_equal(file_count, 0L)

  cleanup_temp_ducklake(lake)
})

test_that("large inserts bypass inlining and write Parquet", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Insert more rows than the default threshold (10)
  large_data <- data.frame(
    id = 1:100,
    value = rnorm(100)
  )
  with_transaction(
    create_table(large_data, "big_table"),
    author = "test",
    commit_message = "bulk load"
  )

  # At least one Parquet file should exist
  conn <- get_ducklake_connection()
  file_count <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'big_table');",
      lake$ducklake_name
    )
  )$n
  expect_gt(file_count, 0L)

  cleanup_temp_ducklake(lake)
})

test_that("set_inlining_row_limit sets global default", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()

  set_inlining_row_limit(50)
  limit <- get_inlining_row_limit()
  expect_equal(limit, 50L)

  # Restore default

  set_inlining_row_limit(10)
  limit <- get_inlining_row_limit()
  expect_equal(limit, 10L)

  cleanup_temp_ducklake(lake)
})

test_that("set_inlining_row_limit rejects negative values", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  expect_error(set_inlining_row_limit(-1), "non-negative")
  cleanup_temp_ducklake(lake)
})

test_that("set_inlining_row_limit(0) disables inlining", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  temp_dir <- tempfile()
  dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)
  ducklake_name <- paste0("test_noinline_", format(Sys.time(), "%Y%m%d%H%M%S"))

  # Disable inlining BEFORE attaching
  set_inlining_row_limit(0)

  attach_ducklake(ducklake_name, lake_path = temp_dir)

  # Create table with a single row — would normally inline
  with_transaction(
    create_table(
      data.frame(id = 1L, val = 42.0),
      "no_inline"
    ),
    commit_message = "single row, inlining disabled"
  )

  # With inlining disabled, the single row should be in a Parquet file
  conn <- get_ducklake_connection()
  file_count <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'no_inline');",
      ducklake_name
    )
  )$n
  expect_gt(file_count, 0L)

  # Restore default and clean up
  set_inlining_row_limit(10)
  detach_ducklake(ducklake_name)
  unlink(temp_dir, recursive = TRUE)
})

test_that("flush_inlined_data materialises inlined rows to Parquet", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Create table with small inlined data
  with_transaction(
    create_table(
      data.frame(sensor_id = 1:2, temperature = c(21.5, 22.1)),
      "flush_test"
    ),
    commit_message = "small inlined table"
  )

  # Before flush: no Parquet files in DuckLake's file list
  conn <- get_ducklake_connection()
  before <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'flush_test');",
      lake$ducklake_name
    )
  )$n
  expect_equal(before, 0L)

  # Flush inlined data
  flush_result <- flush_inlined_data(
    ducklake_name = lake$ducklake_name,
    table_name = "flush_test"
  )
  expect_true(is.data.frame(flush_result))
  expect_gt(nrow(flush_result), 0L)
  expect_true("rows_flushed" %in% names(flush_result))
  expect_equal(sum(flush_result$rows_flushed), 2L)

  # After flush: Parquet files should exist in DuckLake's file list
  after <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'flush_test');",
      lake$ducklake_name
    )
  )$n
  expect_gt(after, 0L)

  # Data should still be queryable
  result <- get_ducklake_table("flush_test") |> dplyr::collect()
  expect_equal(nrow(result), 2)

  cleanup_temp_ducklake(lake)
})

test_that("flush_inlined_data can target a specific table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Create two small inlined tables
  with_transaction(
    create_table(data.frame(id = 1L, val = 1.0), "tbl_a"),
    commit_message = "create tbl_a"
  )
  with_transaction(
    create_table(data.frame(id = 1L, val = 2.0), "tbl_b"),
    commit_message = "create tbl_b"
  )

  # Flush only tbl_a
  flush_result <- flush_inlined_data(
    ducklake_name = lake$ducklake_name,
    table_name = "tbl_a"
  )
  expect_gt(nrow(flush_result), 0L)
  expect_equal(sum(flush_result$rows_flushed), 1L)

  # tbl_a should now have Parquet files (flushed)
  files_a <- DBI::dbGetQuery(
    get_ducklake_connection(),
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'tbl_a');",
      lake$ducklake_name
    )
  )$n
  expect_gt(files_a, 0L)

  # tbl_b should still be inlined (no Parquet files)
  files_b <- DBI::dbGetQuery(
    get_ducklake_connection(),
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'tbl_b');",
      lake$ducklake_name
    )
  )$n
  expect_equal(files_b, 0L)

  # Both tables should still be queryable
  result_a <- get_ducklake_table("tbl_a") |> dplyr::collect()
  result_b <- get_ducklake_table("tbl_b") |> dplyr::collect()
  expect_equal(nrow(result_a), 1)
  expect_equal(nrow(result_b), 1)

  cleanup_temp_ducklake(lake)
})

test_that("checkpoint_ducklake runs maintenance", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Create table with initial data
  with_transaction(
    create_table(
      data.frame(id = 1:3, val = c(10, 20, 30)),
      "checkpoint_test"
    ),
    commit_message = "initial data"
  )

  # Incrementally add a row (exercises inlining)
  rows_insert(
    get_ducklake_table("checkpoint_test"),
    data.frame(id = 4L, val = 40.0),
    by = "id"
  )

  # Before checkpoint: the incremental row should be inlined (no new Parquet file for it)
  conn <- get_ducklake_connection()
  files_before <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'checkpoint_test');",
      lake$ducklake_name
    )
  )$n

  # Checkpoint should complete without error
  expect_no_error(checkpoint_ducklake(lake$ducklake_name))

  # After checkpoint: inlined row should be flushed to Parquet
  files_after <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'checkpoint_test');",
      lake$ducklake_name
    )
  )$n
  expect_gt(files_after, files_before)

  # Data should remain correct
  result <- get_ducklake_table("checkpoint_test") |> dplyr::collect()
  expect_equal(nrow(result), 4)

  cleanup_temp_ducklake(lake)
})

test_that("inlined data supports time travel", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Create table (snapshot 1)
  with_transaction(
    create_table(
      data.frame(id = 1:3, val = c(10, 20, 30)),
      "time_travel_inline"
    ),
    commit_message = "initial data"
  )

  # Get snapshot info after creation
  snapshots_before <- list_table_snapshots(
    "time_travel_inline",
    ducklake_name = lake$ducklake_name
  )

  # Incrementally add a row (exercises true per-row inlining)
  rows_insert(
    get_ducklake_table("time_travel_inline"),
    data.frame(id = 4L, val = 40.0),
    by = "id"
  )

  # Current state should have 4 rows
  current <- get_ducklake_table("time_travel_inline") |> dplyr::collect()
  expect_equal(nrow(current), 4)

  # Snapshots must exist after table creation
  expect_gt(nrow(snapshots_before), 0L)

  # Time travel to first snapshot should show 3 rows
  first_version <- snapshots_before$snapshot_id[1]
  historical <- get_ducklake_table_version(
    "time_travel_inline",
    first_version
  ) |>
    dplyr::collect()
  expect_equal(nrow(historical), 3)

  cleanup_temp_ducklake(lake)
})

test_that("inlined deletes work correctly", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Create table with data
  with_transaction(
    create_table(
      data.frame(id = 1:5, val = c(10, 20, 30, 40, 50)),
      "delete_inline"
    ),
    commit_message = "initial data"
  )

  # Delete a single row (exercises per-row deletion inlining)
  rows_delete(
    get_ducklake_table("delete_inline"),
    data.frame(id = 3L),
    by = "id"
  )

  # Verify deletion
  result <- get_ducklake_table("delete_inline") |> dplyr::collect()
  expect_equal(nrow(result), 4)
  expect_false(3L %in% result$id)

  cleanup_temp_ducklake(lake)
})

# ---------------------------------------------------------------------------
# Per-table and per-schema inlining configuration
# ---------------------------------------------------------------------------

test_that("set/get_inlining_row_limit works per-table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Create a table so the per-table option has a target
  with_transaction(
    create_table(data.frame(id = 1:2, val = c(10, 20)), "per_tbl_test"),
    commit_message = "create per_tbl_test"
  )

  # Set per-table limit (auto-detects ducklake_name)
  set_inlining_row_limit(75, table_name = "per_tbl_test")

  # Query per-table limit (auto-detects ducklake_name)
  limit <- get_inlining_row_limit(table_name = "per_tbl_test")
  expect_equal(limit, 75L)

  cleanup_temp_ducklake(lake)
})

test_that("set/get_inlining_row_limit works per-schema", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Set per-schema limit
  set_inlining_row_limit(30, schema_name = "main")

  # Query per-schema limit
  limit <- get_inlining_row_limit(schema_name = "main")
  expect_equal(limit, 30L)

  cleanup_temp_ducklake(lake)
})

test_that("set/get_inlining_row_limit works with table_name and schema_name", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(data.frame(id = 1L, val = 1.0), "combo_test"),
    commit_message = "create combo_test"
  )

  # Set with both table and schema, explicit ducklake_name
  set_inlining_row_limit(
    200,
    table_name = "combo_test",
    schema_name = "main",
    ducklake_name = lake$ducklake_name
  )

  # Query with both table and schema, explicit ducklake_name
  limit <- get_inlining_row_limit(
    table_name = "combo_test",
    schema_name = "main",
    ducklake_name = lake$ducklake_name
  )
  expect_equal(limit, 200L)

  cleanup_temp_ducklake(lake)
})

test_that("flush_inlined_data reports no data when nothing is inlined", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Create a large table that bypasses inlining entirely
  with_transaction(
    create_table(data.frame(id = 1:100, val = rnorm(100)), "big"),
    commit_message = "big table"
  )

  # Flushing the specific table should return 0 rows (nothing inlined)
  flush_result <- flush_inlined_data(
    ducklake_name = lake$ducklake_name,
    table_name = "big"
  )
  expect_true(is.data.frame(flush_result))
  expect_equal(nrow(flush_result), 0L)

  cleanup_temp_ducklake(lake)
})

test_that("flush_inlined_data works with schema_name filter", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # Create a small inlined table
  with_transaction(
    create_table(data.frame(id = 1:2, val = c(1, 2)), "schema_flush"),
    commit_message = "small table for schema flush"
  )

  # Before flush: data is inlined (no Parquet files)
  conn <- get_ducklake_connection()
  before <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'schema_flush');",
      lake$ducklake_name
    )
  )$n
  expect_equal(before, 0L)

  # Flush by schema
  flush_result <- flush_inlined_data(
    ducklake_name = lake$ducklake_name,
    schema_name = "main"
  )
  expect_true(is.data.frame(flush_result))
  expect_gt(nrow(flush_result), 0L)
  expect_gt(sum(flush_result$rows_flushed), 0L)

  # After flush: Parquet files should exist
  after <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'schema_flush');",
      lake$ducklake_name
    )
  )$n
  expect_gt(after, 0L)

  # Data should still be queryable
  result <- get_ducklake_table("schema_flush") |> dplyr::collect()
  expect_equal(nrow(result), 2)

  cleanup_temp_ducklake(lake)
})

test_that("flush_inlined_data works with table_name and schema_name", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(data.frame(id = 1:3, val = c(1, 2, 3)), "combo_flush"),
    commit_message = "table for combo flush"
  )

  # Before flush: data is inlined
  conn <- get_ducklake_connection()
  before <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'combo_flush');",
      lake$ducklake_name
    )
  )$n
  expect_equal(before, 0L)

  # Flush with both table and schema
  flush_result <- flush_inlined_data(
    ducklake_name = lake$ducklake_name,
    table_name = "combo_flush",
    schema_name = "main"
  )
  expect_true(is.data.frame(flush_result))
  expect_gt(nrow(flush_result), 0L)
  expect_equal(sum(flush_result$rows_flushed), 3L)

  # After flush: Parquet files should exist
  after <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'combo_flush');",
      lake$ducklake_name
    )
  )$n
  expect_gt(after, 0L)

  result <- get_ducklake_table("combo_flush") |> dplyr::collect()
  expect_equal(nrow(result), 3)

  cleanup_temp_ducklake(lake)
})

test_that("checkpoint_ducklake auto-detects ducklake_name", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(data.frame(id = 1:2, val = c(10, 20)), "ckpt_auto"),
    commit_message = "checkpoint auto-detect test"
  )

  # Before checkpoint: data is inlined
  conn <- get_ducklake_connection()
  before <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'ckpt_auto');",
      lake$ducklake_name
    )
  )$n
  expect_equal(before, 0L)

  # Call without explicit ducklake_name — should auto-detect
  expect_no_error(checkpoint_ducklake())

  # After checkpoint: inlined data should be flushed to Parquet
  after <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'ckpt_auto');",
      lake$ducklake_name
    )
  )$n
  expect_gt(after, 0L)

  result <- get_ducklake_table("ckpt_auto") |> dplyr::collect()
  expect_equal(nrow(result), 2)

  cleanup_temp_ducklake(lake)
})

test_that("flush_inlined_data auto-detects ducklake_name", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(data.frame(id = 1:2, val = c(5, 6)), "flush_auto"),
    commit_message = "flush auto-detect test"
  )

  # Before flush: data is inlined
  conn <- get_ducklake_connection()
  before <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'flush_auto');",
      lake$ducklake_name
    )
  )$n
  expect_equal(before, 0L)

  # Call without explicit ducklake_name
  flush_result <- flush_inlined_data()
  expect_true(is.data.frame(flush_result))
  expect_gt(nrow(flush_result), 0L)

  # After flush: Parquet files should exist
  after <- DBI::dbGetQuery(
    conn,
    sprintf(
      "SELECT count(*) AS n FROM ducklake_list_files('%s', 'flush_auto');",
      lake$ducklake_name
    )
  )$n
  expect_gt(after, 0L)

  result <- get_ducklake_table("flush_auto") |> dplyr::collect()
  expect_equal(nrow(result), 2)

  cleanup_temp_ducklake(lake)
})
