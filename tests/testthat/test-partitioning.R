# Tests for partitioning support: set_table_partitioning,
# reset_table_partitioning, and get_table_partitions.

test_that("set_table_partitioning validates expressions before building SQL", {
  skip_if_not_installed("duckdb")

  expect_error(set_table_partitioning("t", character(0)), "character vector")
  expect_error(set_table_partitioning("t", NA_character_), "character vector")
  expect_error(set_table_partitioning("t", 1L), "character vector")
  expect_error(
    set_table_partitioning("t", "1; DROP TABLE x"),
    "Invalid partition expression"
  )
  expect_error(
    set_table_partitioning("t", "year(ts); DROP TABLE x"),
    "Invalid partition expression"
  )
  expect_error(
    set_table_partitioning("t", c("id", "minute(ts)")),
    "Invalid partition expression"
  )
  expect_error(
    set_table_partitioning("t", "bucket(x, id)"),
    "Invalid partition expression"
  )
})

test_that("partitioning keys round-trip through set, get, and reset", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  # Write real Parquet files so partition directories appear on disk
  suppressMessages(set_inlining_row_limit(0))
  on.exit(suppressMessages(set_inlining_row_limit(10)), add = TRUE)

  create_table(
    data.frame(
      id = 1:3,
      ts = as.POSIXct("2024-03-15 10:00:00", tz = "UTC") + 1:3
    ),
    "part_tbl"
  )

  expect_equal(nrow(get_table_partitions("part_tbl")), 0)

  set_table_partitioning("part_tbl", c("year(ts)", "month(ts)"))
  keys <- get_table_partitions("part_tbl")
  expect_equal(keys$transform, c("year", "month"))
  expect_equal(keys$column_name, c("ts", "ts"))
  expect_equal(keys$partition_key_index, c(0, 1))

  # Data written after the keys are set lands in partitioned directories
  rows_insert(
    get_ducklake_table("part_tbl"),
    data.frame(id = 99L, ts = as.POSIXct("2025-06-15", tz = "UTC")),
    by = "id"
  )
  dirs <- list.dirs(
    file.path(lake$temp_dir, "main", "part_tbl"),
    recursive = TRUE, full.names = FALSE
  )
  expect_true(any(grepl("^year=2025", dirs)))

  # Re-setting replaces the keys; bucket transforms are supported
  set_table_partitioning("part_tbl", "bucket(4, id)")
  keys <- get_table_partitions("part_tbl")
  expect_equal(keys$transform, "bucket(4)")
  expect_equal(keys$column_name, "id")

  reset_table_partitioning("part_tbl")
  expect_equal(nrow(get_table_partitions("part_tbl")), 0)
})

test_that("get_table_partitions lists all partitioned tables in the lake", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, grp = c("a", "b", "a")), "part_a")
  create_table(data.frame(id = 1:3), "part_b")

  set_table_partitioning("part_a", "grp")
  set_table_partitioning("part_b", "id")

  all_keys <- get_table_partitions()
  expect_setequal(all_keys$table_name, c("part_a", "part_b"))
  expect_equal(
    all_keys$transform[all_keys$table_name == "part_a"],
    "identity"
  )

  # Filtered listing returns only the requested table
  expect_equal(get_table_partitions("part_a")$column_name, "grp")
})
