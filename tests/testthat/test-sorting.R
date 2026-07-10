# Tests for set_table_sorting() / reset_table_sorting()

test_that("sorting keys round-trip through set and reset", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = c(3L, 1L, 2L), v = c("c", "a", "b")), "sorted_t")
  expect_message(
    set_table_sorting("sorted_t", c("id ASC", "v DESC NULLS LAST")),
    "now sorted by"
  )

  # New writes go through the sort path without error
  suppressMessages(
    rows_insert(
      get_ducklake_table("sorted_t"),
      data.frame(id = 4L, v = "d"),
      by = "id"
    )
  )
  expect_equal(nrow(dplyr::collect(get_ducklake_table("sorted_t"))), 4)

  expect_message(reset_table_sorting("sorted_t"), "removed")

  cleanup_temp_ducklake(lake)
})

test_that("set_table_sorting rejects expressions outside the allowlist", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()

  expect_error(
    set_table_sorting("t", "id; DROP TABLE t"),
    "Invalid sort expression"
  )
  expect_error(
    set_table_sorting("t", "date_trunc('hour', ts)"),
    "Invalid sort expression"
  )
  expect_error(set_table_sorting("t", character(0)), "character vector")

  cleanup_temp_ducklake(lake)
})
