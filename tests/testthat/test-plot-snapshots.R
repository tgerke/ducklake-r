# Tests for plot_snapshots() and its changes classifier

test_that("plot_snapshots returns a ggplot with one row per snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("ggplot2")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(mtcars[1:5, ], "plot_snap_1"),
    author = "Analyst",
    commit_message = "Initial load"
  )
  with_transaction(
    replace_table(mtcars[1:10, ], "plot_snap_1", .quiet = TRUE),
    author = "Analyst",
    commit_message = "Add five more rows"
  )

  p <- plot_snapshots("plot_snap_1")

  expect_s3_class(p, "ggplot")
  snapshots <- list_table_snapshots("plot_snap_1")
  expect_equal(nrow(p$data), nrow(snapshots))
  expect_true(all(c("change_type", "snapshot_label", "annotation") %in% names(p$data)))
  expect_equal(
    p$data$annotation[p$data$snapshot_id == snapshots$snapshot_id[1]],
    "Analyst: Initial load"
  )

  cleanup_temp_ducklake(lake)
})

test_that("plot_snapshots works lake-wide with no table_name", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("ggplot2")

  lake <- create_temp_ducklake()

  create_table(mtcars[1:3, ], "plot_snap_all")

  p <- plot_snapshots(ducklake_name = lake$ducklake_name)

  expect_s3_class(p, "ggplot")
  expect_gte(nrow(p$data), 1)

  cleanup_temp_ducklake(lake)
})

test_that("plot_snapshots errors informatively when there are no snapshots", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("ggplot2")

  lake <- create_temp_ducklake()

  expect_error(
    suppressWarnings(plot_snapshots("no_such_table")),
    "No snapshots found"
  )

  cleanup_temp_ducklake(lake)
})

test_that("classify_snapshot_changes maps tokens in priority order", {
  result <- classify_snapshot_changes(c(
    "tables_created, tables_inserted_into, main.dm_raw, 1",
    "tables_inserted_into, 1",
    "inlined_insert, 1",
    "tables_deleted_from, 2",
    "tables_altered, 2",
    "tables_dropped, 3",
    "flushed_inlined, 1",
    "compacted, 1",
    "something_unrecognized",
    NA
  ))

  expect_s3_class(result, "factor")
  expect_equal(
    levels(result),
    c("created", "schema change", "data change", "maintenance", "other")
  )
  expect_equal(
    as.character(result),
    c(
      "created", "data change", "data change", "data change",
      "schema change", "schema change", "maintenance", "maintenance",
      "other", "other"
    )
  )
})
