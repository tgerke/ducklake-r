# Tests for snapshot-pinned attach (SNAPSHOT_VERSION / SNAPSHOT_TIME)

test_that("build_attach_sql renders snapshot pinning options", {
  sql <- build_attach_sql(
    "lake", "/data", "duckdb", NULL,
    read_only = FALSE, snapshot_version = 12
  )
  expect_match(sql, "SNAPSHOT_VERSION 12", fixed = TRUE)

  sql <- build_attach_sql(
    "lake", "/data", "duckdb", NULL,
    read_only = FALSE,
    snapshot_time = as.POSIXct("2026-05-26 00:00:00", tz = "UTC")
  )
  expect_match(sql, "SNAPSHOT_TIME '2026-05-26 00:00:00", fixed = TRUE)
})

test_that("attach_ducklake rejects both pinning arguments at once", {
  expect_error(
    attach_ducklake(
      "pinned_both",
      lake_path = tempdir(),
      snapshot_version = 1,
      snapshot_time = "2026-01-01 00:00:00"
    ),
    "only one of"
  )
})

test_that("a snapshot-pinned attach sees the lake as of that snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2), "pin_t")
  v1 <- max(list_table_snapshots("pin_t")$snapshot_id)
  suppressMessages(
    rows_insert(get_ducklake_table("pin_t"), data.frame(id = 3L), by = "id")
  )
  expect_equal(nrow(dplyr::collect(get_ducklake_table("pin_t"))), 3)

  detach_ducklake(lake$ducklake_name)
  attach_ducklake(
    lake$ducklake_name,
    lake_path = lake$lake_path,
    snapshot_version = v1
  )

  pinned <- dplyr::collect(get_ducklake_table("pin_t"))
  expect_equal(nrow(pinned), 2)
  expect_setequal(pinned$id, 1:2)

  # A pinned lake rejects writes
  expect_error(create_table(data.frame(x = 1), "pin_new"))

  cleanup_temp_ducklake(lake)
})
