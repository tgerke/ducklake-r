# Tests for plot_table_files() and its byte formatter

test_that("plot_table_files draws one bar segment per table and file kind", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("ggplot2")

  lake <- create_temp_ducklake()

  create_table(mtcars, "files_big")
  create_table(data.frame(x = 1:50), "files_small")

  p <- plot_table_files()

  expect_s3_class(p, "ggplot")
  expect_equal(nrow(p$data), 4)
  expect_setequal(unique(p$data$table_name), c("files_big", "files_small"))
  expect_setequal(levels(p$data$kind), c("data files", "delete files"))

  cleanup_temp_ducklake(lake)
})

test_that("plot_table_files errors informatively when the lake has no tables", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("ggplot2")

  lake <- create_temp_ducklake()

  expect_error(plot_table_files(), "No tables found")

  cleanup_temp_ducklake(lake)
})

test_that("format_bytes produces readable sizes", {
  expect_equal(
    format_bytes(c(0, 512, 2913, 20000, 5.2e6, 1.4e9, NA)),
    c("0 B", "512 B", "2.9 kB", "20 kB", "5.2 MB", "1.4 GB", NA)
  )
})

test_that("plot_table_files treats NA file stats as zero", {
  skip_if_not_installed("ggplot2")

  local_mocked_bindings(
    get_ducklake_connection = function() NULL,
    infer_ducklake_name = function(ducklake_name, conn) "mock_lake",
    get_table_info = function(...) {
      data.frame(
        table_name = c("inlined_tbl", "on_disk"),
        file_count = c(NA, 2),
        file_size_bytes = c(NA, 1000),
        delete_file_count = c(NA, 0),
        delete_file_size_bytes = c(NA, 0)
      )
    }
  )

  p <- plot_table_files()

  expect_s3_class(p, "ggplot")
  expect_equal(sum(p$data$bytes), 1000)
})
