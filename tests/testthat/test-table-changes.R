# Tests for the data change feed wrapper: get_table_changes.

test_that("get_table_changes returns inserts, updates, and deletes", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, v = c(10, 20, 30)), "cdc")
  rows_update(get_ducklake_table("cdc"), data.frame(id = 2L, v = 99), by = "id")
  rows_delete(get_ducklake_table("cdc"), data.frame(id = 3L), by = "id")

  snaps <- list_table_snapshots("cdc")
  lo <- min(snaps$snapshot_id)
  hi <- max(snaps$snapshot_id)

  changes <- get_table_changes("cdc", lo, hi) |> dplyr::collect()
  expect_setequal(
    unique(changes$change_type),
    c("insert", "update_preimage", "update_postimage", "delete")
  )
  expect_equal(sum(changes$change_type == "insert"), 3)

  # Updates carry both the before and after image of the row
  expect_equal(changes$v[changes$change_type == "update_preimage"], 20)
  expect_equal(changes$v[changes$change_type == "update_postimage"], 99)
  expect_equal(changes$v[changes$change_type == "delete"], 30)

  # The result is lazy: dplyr verbs compose before collecting
  deletes <- get_table_changes("cdc", lo, hi) |>
    dplyr::filter(change_type == "delete") |>
    dplyr::collect()
  expect_equal(nrow(deletes), 1)
  expect_equal(deletes$id, 3)

  # Timestamp bounds cover the same range (snapshot times are UTC)
  changes_t <- get_table_changes(
    "cdc",
    min(snaps$snapshot_time),
    max(snaps$snapshot_time) + 1
  ) |>
    dplyr::collect()
  expect_equal(nrow(changes_t), nrow(changes))
})

test_that("get_table_changes narrows to a single snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:2), "cdc_one")
  rows_insert(get_ducklake_table("cdc_one"), data.frame(id = 3L), by = "id")

  snaps <- list_table_snapshots("cdc_one")
  last <- max(snaps$snapshot_id)

  changes <- get_table_changes("cdc_one", last, last) |> dplyr::collect()
  expect_equal(changes$change_type, "insert")
  expect_equal(changes$id, 3)
})

test_that("get_table_changes rejects mixed bound types", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1), "cdc_mixed")

  expect_error(get_table_changes("cdc_mixed", 1, Sys.time()), "both")
  expect_error(
    get_table_changes("cdc_mixed", "2024-01-01", 5),
    "both"
  )
})
