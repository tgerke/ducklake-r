# Tests for set_ducklake_option() / get_ducklake_options()

test_that("options round-trip at lake and table scope", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()

  suppressMessages(set_ducklake_option("parquet_compression", "zstd"))
  opts <- get_ducklake_options()
  row <- opts[opts$option_name == "parquet_compression", ]
  expect_equal(nrow(row), 1)
  expect_equal(row$value, "zstd")
  expect_equal(row$scope, "GLOBAL")

  create_table(data.frame(x = 1:3), "opt_target")
  suppressMessages(
    set_ducklake_option("auto_compact", FALSE, table_name = "opt_target")
  )
  opts <- get_ducklake_options()
  row <- opts[opts$option_name == "auto_compact", ]
  expect_equal(nrow(row), 1)
  expect_equal(row$scope, "TABLE")
  expect_match(row$scope_entry, "opt_target")

  cleanup_temp_ducklake(lake)
})

test_that("set_ducklake_option validates its inputs", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()

  expect_error(
    set_ducklake_option("bad option!", "x"),
    "snake_case"
  )
  expect_error(
    set_ducklake_option("parquet_compression", c("a", "b")),
    "single non-missing value"
  )

  cleanup_temp_ducklake(lake)
})

test_that("render_option_value renders logicals, numbers, and strings", {
  expect_equal(render_option_value(TRUE), "true")
  expect_equal(render_option_value(FALSE), "false")
  expect_equal(render_option_value(122880), "122880")
  expect_equal(render_option_value("zstd"), "'zstd'")
  expect_equal(render_option_value("o'clock"), "'o''clock'")
})

test_that("option_scope_args renders each scoping combination", {
  expect_equal(option_scope_args(NULL, NULL), "")
  expect_equal(option_scope_args("t", NULL), ", table_name => 't'")
  expect_equal(option_scope_args(NULL, "s"), ", schema => 's'")
  expect_equal(
    option_scope_args("t", "s"),
    ", schema => 's', table_name => 't'"
  )
})
