# Tests for plot_table_changes()

test_that("plot_table_changes counts rows by snapshot and kind", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("ggplot2")

  lake <- create_temp_ducklake()

  create_table(mtcars[1:5, ], "chg_tbl")
  rows_insert(get_ducklake_table("chg_tbl"), mtcars[6:8, ], by = "mpg")
  rows_delete(get_ducklake_table("chg_tbl"), mtcars[1:2, "mpg", drop = FALSE], by = "mpg")
  rows_update(
    get_ducklake_table("chg_tbl"),
    data.frame(mpg = mtcars$mpg[3], hp = 999),
    by = "mpg"
  )

  p <- plot_table_changes("chg_tbl")

  expect_s3_class(p, "ggplot")
  d <- p$data
  expect_equal(sum(d$n[d$kind == "inserted"]), 8)
  expect_equal(sum(d$n[d$kind == "deleted"]), 2)
  expect_equal(sum(d$n[d$kind == "updated"]), 1)
  expect_true(all(d$rows[d$kind == "deleted"] < 0))
  expect_true(all(d$rows[d$kind != "deleted"] > 0))

  # Every snapshot of the table keeps a slot on the axis
  snapshots <- list_table_snapshots("chg_tbl")
  expect_setequal(levels(d$snapshot_label), as.character(snapshots$snapshot_id))

  cleanup_temp_ducklake(lake)
})

test_that("plot_table_changes errors informatively when there are no snapshots", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("ggplot2")

  lake <- create_temp_ducklake()

  expect_error(
    suppressWarnings(plot_table_changes("no_such_table")),
    "No snapshots found"
  )

  cleanup_temp_ducklake(lake)
})
