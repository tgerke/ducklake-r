test_that("get_ducklake_info() describes the attached lake", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  create_table(mtcars, "cars")

  info <- get_ducklake_info()
  expect_s3_class(info, "data.frame")
  expect_equal(nrow(info), 1)
  expect_equal(info$ducklake_name, lake$ducklake_name)
  expect_equal(info$backend, "duckdb")
  expect_true(file.exists(info$catalog))
  expect_true(startsWith(normalizePath(info$data_path), normalizePath(lake$lake_path)))
  expect_true(nzchar(info$format_version))
  expect_true(nzchar(info$extension_version))
  expect_false(info$encrypted)
  expect_equal(info$current_snapshot, max(list_table_snapshots()$snapshot_id))

  # By name, too
  expect_equal(get_ducklake_info(lake$ducklake_name)$current_snapshot, info$current_snapshot)
})
