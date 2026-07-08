# Tests for get_table_info()

test_that("get_table_info reports per-table file statistics", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  # mtcars is large enough that its rows are not inlined, so a file is written
  create_table(mtcars, "info_cars")
  create_table(data.frame(x = 1:3), "info_tiny")

  info <- get_table_info()

  expect_s3_class(info, "data.frame")
  expect_setequal(info$table_name, c("info_cars", "info_tiny"))
  expect_true(all(
    c("file_count", "file_size_bytes", "delete_file_count", "delete_file_size_bytes") %in%
      names(info)
  ))
  cars_row <- info[info$table_name == "info_cars", ]
  expect_gte(cars_row$file_count, 1)
  expect_gt(cars_row$file_size_bytes, 0)

  cleanup_temp_ducklake(lake)
})

test_that("get_table_info filters to a single table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(mtcars, "info_cars")
  create_table(data.frame(x = 1:3), "info_tiny")

  one <- get_table_info("info_cars")

  expect_equal(nrow(one), 1)
  expect_equal(one$table_name, "info_cars")

  cleanup_temp_ducklake(lake)
})
