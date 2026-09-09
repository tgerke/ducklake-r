# Tests for plot_snapshots() and its changes classifier

test_that("plot_snapshots draws a commit log with one row per snapshot", {
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
  expect_true(all(
    c("change_type", "row", "id_label", "time_label", "annotation") %in%
      names(p$data)
  ))
  expect_equal(
    p$data$annotation[p$data$snapshot_id == snapshots$snapshot_id[1]],
    "Analyst: Initial load"
  )
  # Newest snapshot gets the highest row so it draws at the top
  expect_equal(
    p$data$snapshot_id[which.max(p$data$row)],
    max(snapshots$snapshot_id)
  )

  cleanup_temp_ducklake(lake)
})

test_that("plot_snapshots draws a lake-wide swimlane with a lane per table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("ggplot2")

  lake <- create_temp_ducklake()

  create_table(mtcars[1:3, ], "plot_snap_all")
  create_table(mtcars[1:2, ], "plot_snap_other")

  p <- plot_snapshots(ducklake_name = lake$ducklake_name)

  expect_s3_class(p, "ggplot")
  expect_true(all(c("table", "order", "change_type") %in% names(p$data)))
  expect_true(all(
    c("plot_snap_all", "plot_snap_other") %in% levels(p$data$table)
  ))
  # The schema-creation snapshot has no table and lands in the (lake) lane
  expect_true("(lake)" %in% levels(p$data$table))

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

test_that("snapshot_table_changes gives each table its own change type", {
  changes <- list(
    data.frame(key = "schemas_created", value = I(list("main"))),
    data.frame(
      key = c("tables_created", "inlined_insert"),
      value = I(list("main.fleet", "1"))
    ),
    data.frame(key = "inlined_insert", value = I(list("1"))),
    data.frame(key = "tables_inserted_into", value = I(list("99"))),
    # One transaction: rows updated in fleet, crew rebuilt (drop + create)
    data.frame(
      key = c("tables_created", "tables_dropped", "inlined_insert", "inlined_delete"),
      value = I(list("main.crew", "2", c("1", "3"), "1"))
    )
  )
  id_names <- c("1" = "main.fleet", "2" = "main.crew", "3" = "main.crew")

  result <- snapshot_table_changes(changes, id_names)

  expect_equal(result[[1]]$table, "(lake)")
  expect_equal(result[[1]]$change_type, "created")
  expect_equal(result[[2]]$table, "main.fleet")
  expect_equal(result[[2]]$change_type, "created")
  expect_equal(result[[3]]$change_type, "data change")
  # Unknown ids keep a placeholder lane rather than being dropped
  expect_equal(result[[4]]$table, "table 99")
  multi <- result[[5]]
  expect_setequal(multi$table, c("main.crew", "main.fleet"))
  expect_equal(multi$change_type[multi$table == "main.fleet"], "data change")
  expect_equal(multi$change_type[multi$table == "main.crew"], "created")
})

test_that("classify_snapshot_changes can classify by what a snapshot did to one table", {
  changes <- list(data.frame(
    key = c("tables_created", "tables_dropped", "inlined_insert", "inlined_delete"),
    value = I(list("silver.dm", "4", c("5", "6"), "5"))
  ))

  expect_equal(as.character(classify_snapshot_changes(changes)), "created")
  expect_equal(
    as.character(classify_snapshot_changes(changes, targets = c("silver.dm", "4", "6"))),
    "created"
  )
  expect_equal(
    as.character(classify_snapshot_changes(changes, targets = c("main.t", "5"))),
    "data change"
  )
  expect_equal(
    as.character(classify_snapshot_changes(changes, targets = "99")),
    "other"
  )
})

test_that("display_table_names qualifies lanes only when tables leave main", {
  expect_equal(
    display_table_names(c("main.fleet", "main.crew", "(lake)", "table 99")),
    c("fleet", "crew", "(lake)", "table 99")
  )
  expect_equal(
    display_table_names(c("bronze.dm", "silver.dm", "main.notes", "(lake)")),
    c("bronze.dm", "silver.dm", "main.notes", "(lake)")
  )
  expect_equal(display_table_names("(lake)"), "(lake)")
})

test_that("format_gap_duration picks sensible units", {
  expect_equal(format_gap_duration(45 * 60), "45 minutes")
  expect_equal(format_gap_duration(18 * 3600), "18 hours")
  expect_equal(format_gap_duration(103 * 86400), "103 days")
})

test_that("commit log marks large gaps between snapshots", {
  skip_if_not_installed("ggplot2")

  base <- as.POSIXct("2026-03-10 09:00:00", tz = "UTC")
  snapshots <- data.frame(
    snapshot_id = 1:4,
    snapshot_time = base + c(0, 60, 120, 103 * 86400),
    author = NA_character_,
    commit_message = NA_character_
  )
  snapshots$changes <- I(as.list(rep("inlined_insert, 1", 4)))
  snapshots$change_type <- classify_snapshot_changes(snapshots$changes)

  p <- plot_snapshot_commit_log(snapshots, "gappy")

  gap_layer_data <- lapply(p$layers, function(l) l$data)
  gap_labels <- unlist(lapply(gap_layer_data, function(d) {
    if (is.data.frame(d) && "label" %in% names(d)) d$label else NULL
  }))
  expect_length(gap_labels, 1)
  expect_match(gap_labels, "103 days later")
})

test_that("commit log handles a single snapshot without metadata columns", {
  skip_if_not_installed("ggplot2")

  snapshots <- data.frame(
    snapshot_id = 1,
    snapshot_time = as.POSIXct("2026-03-10 09:00:00", tz = "UTC")
  )
  snapshots$changes <- I(list("tables_created, main.solo"))
  snapshots$change_type <- classify_snapshot_changes(snapshots$changes)

  expect_no_warning(p <- plot_snapshot_commit_log(snapshots, "solo"))

  expect_s3_class(p, "ggplot")
  expect_true(all(is.na(p$data$annotation)))
})

test_that("a multi-table transaction is classified per table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("ggplot2")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:3, v = c("a", "b", "c")), "snap_updated")
  create_table(data.frame(id = 1:3), "snap_replaced")
  with_transaction({
    rows_update(
      get_ducklake_table("snap_updated"),
      data.frame(id = 1, v = "z"),
      by = "id"
    )
    replace_table(data.frame(id = 1:5), "snap_replaced", .quiet = TRUE)
  })
  latest <- max(list_table_snapshots()$snapshot_id)

  updated <- plot_snapshots("snap_updated")$data
  expect_equal(
    as.character(updated$change_type[updated$snapshot_id == latest]),
    "data change"
  )
  replaced <- plot_snapshots("snap_replaced")$data
  expect_equal(
    as.character(replaced$change_type[replaced$snapshot_id == latest]),
    "created"
  )

  lanes <- plot_snapshots(ducklake_name = lake$ducklake_name)$data
  lanes <- lanes[lanes$snapshot_id == latest, ]
  expect_equal(
    as.character(lanes$change_type[lanes$table == "snap_updated"]),
    "data change"
  )
  expect_equal(
    as.character(lanes$change_type[lanes$table == "snap_replaced"]),
    "created"
  )

  cleanup_temp_ducklake(lake)
})

test_that("swimlane lanes carry schema names when tables live in several schemas", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  skip_if_not_installed("ggplot2")

  lake <- create_temp_ducklake()

  create_schema("bronze")
  create_schema("silver")
  create_table(mtcars[1:3, ], "bronze.dm")
  create_table(mtcars[1:3, ], "silver.dm")

  d <- plot_snapshots(ducklake_name = lake$ducklake_name)$data
  expect_true(all(c("bronze.dm", "silver.dm") %in% levels(d$table)))
  expect_false("dm" %in% levels(d$table))
  # Each table was created once, so each lane holds one point
  expect_equal(sum(d$table == "bronze.dm"), 1L)
  expect_equal(sum(d$table == "silver.dm"), 1L)

  cleanup_temp_ducklake(lake)
})
