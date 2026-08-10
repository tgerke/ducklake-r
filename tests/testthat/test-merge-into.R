# UNIT TESTS: merge_into()
#
# These tests verify the full MERGE surface: conditional clauses, matched
# deletes, and source-driven deletes (delete_missing), including the
# transactional fallback for DuckLake's single update/delete action limit.

test_that("merge_into applies a conditional update", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(
    data.frame(id = 1:2, value = c("a", "b"), amount = c(10, 20)),
    "test_merge_cond"
  )

  n <- merge_into(
    get_ducklake_table("test_merge_cond"),
    data.frame(id = 1:2, value = c("low", "high"), amount = c(5, 100)),
    by = "id",
    matched_condition = "source.amount > target.amount",
    when_not_matched = "nothing"
  )

  expect_equal(n, 1)
  result <- get_ducklake_table("test_merge_cond") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$value, c("a", "high"))
  expect_equal(result$amount, c(10, 100))

  cleanup_temp_ducklake(lake)
})

test_that("merge_into deletes matched rows", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:4, value = letters[1:4]), "test_merge_del")

  n <- merge_into(
    "test_merge_del",
    data.frame(id = c(2L, 4L)),
    by = "id",
    when_matched = "delete",
    when_not_matched = "nothing"
  )

  expect_equal(n, 2)
  result <- get_ducklake_table("test_merge_del") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$id, c(1L, 3L))

  cleanup_temp_ducklake(lake)
})

test_that("merge_into with delete_missing synchronizes in one snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:4, value = letters[1:4]), "test_merge_sync")
  before <- max(list_table_snapshots()$snapshot_id)

  # update + delete_missing exceeds DuckLake's single update/delete action
  # limit, so this exercises the MERGE + DELETE fallback in one transaction
  source <- data.frame(id = c(1L, 3L, 9L), value = c("A", "C", "i"))
  merge_into("test_merge_sync", source, by = "id", delete_missing = TRUE)

  after <- max(list_table_snapshots()$snapshot_id)
  expect_equal(after - before, 1)

  result <- get_ducklake_table("test_merge_sync") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$id, c(1L, 3L, 9L))
  expect_equal(result$value, c("A", "C", "i"))

  cleanup_temp_ducklake(lake)
})

test_that("merge_into accepts a lazy table target and a lazy source", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, value = c("a", "b")), "test_merge_tgt")
  create_table(data.frame(id = 2:3, value = c("B", "c")), "test_merge_src")

  merge_into(
    get_ducklake_table("test_merge_tgt"),
    get_ducklake_table("test_merge_src"),
    by = "id"
  )

  result <- get_ducklake_table("test_merge_tgt") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$id, 1:3)
  expect_equal(result$value, c("a", "B", "c"))

  cleanup_temp_ducklake(lake)
})

test_that("merge_into records the full change mix in the change feed", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:3, value = c("a", "b", "c")), "test_merge_feed")

  merge_into(
    "test_merge_feed",
    data.frame(id = c(1L, 9L), value = c("A", "i")),
    by = "id",
    delete_missing = TRUE
  )

  latest <- max(list_table_snapshots()$snapshot_id)
  changes <- get_table_changes("test_merge_feed", latest, latest) |>
    dplyr::collect()
  expect_setequal(
    changes$change_type,
    c("insert", "update_preimage", "update_postimage", "delete")
  )

  cleanup_temp_ducklake(lake)
})

test_that("merge_into tolerates extra source columns when not inserting", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, value = c("a", "b")), "test_merge_extra")

  # remove_flag exists only in the source and drives the condition
  n <- merge_into(
    "test_merge_extra",
    data.frame(id = 1:2, remove_flag = c(TRUE, FALSE)),
    by = "id",
    when_matched = "delete",
    matched_condition = "source.remove_flag",
    when_not_matched = "nothing"
  )

  expect_equal(n, 1)
  result <- get_ducklake_table("test_merge_extra") |> dplyr::collect()
  expect_equal(result$id, 2L)

  # but inserting with extra source columns is rejected up front
  expect_error(
    merge_into(
      "test_merge_extra",
      data.frame(id = 9L, extra = 1),
      by = "id"
    ),
    "must exist in"
  )

  cleanup_temp_ducklake(lake)
})

test_that("merge_into validates conditions and argument combinations", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, value = c("a", "b")), "test_merge_valid")

  # dead condition
  expect_error(
    merge_into(
      "test_merge_valid", data.frame(id = 1L), by = "id",
      when_matched = "nothing", when_not_matched = "nothing",
      delete_missing = TRUE, matched_condition = "1 = 1"
    ),
    "has no effect"
  )

  # statement chaining in a condition
  expect_error(
    merge_into(
      "test_merge_valid", data.frame(id = 1L, value = "x"), by = "id",
      matched_condition = "1 = 1; DROP TABLE test_merge_valid"
    ),
    "without"
  )

  # nothing to do at all
  expect_error(
    merge_into(
      "test_merge_valid", data.frame(id = 1L), by = "id",
      when_matched = "nothing", when_not_matched = "nothing"
    ),
    "Nothing to do"
  )

  # update with no shared non-key columns
  expect_error(
    merge_into(
      "test_merge_valid", data.frame(id = 1L), by = "id",
      when_not_matched = "nothing"
    ),
    "no non-key columns"
  )

  # by column absent
  expect_error(
    merge_into(
      "test_merge_valid", data.frame(nope = 1L, value = "x"), by = "nope"
    ),
    "must exist in both"
  )

  # the semicolon rejection means the injection-shaped condition above
  # never reached the database
  result <- get_ducklake_table("test_merge_valid") |> dplyr::collect()
  expect_equal(nrow(result), 2)

  cleanup_temp_ducklake(lake)
})
