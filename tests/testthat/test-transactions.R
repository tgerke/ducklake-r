# UNIT TESTS: transaction confirmations
#
# begin_transaction() is silent. commit_transaction() reports the snapshot
# the transaction created, with its author and message, or says that
# nothing changed.

capture_ducklake_messages <- function(expr) {
  msgs <- character()
  withCallingHandlers(
    expr,
    message = function(m) {
      msgs <<- c(msgs, conditionMessage(m))
      invokeRestart("muffleMessage")
    }
  )
  msgs
}

test_that("commit_transaction reports the snapshot it created", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  expect_silent(begin_transaction())
  create_table(data.frame(id = 1:3), "confirm_t")
  msgs <- capture_ducklake_messages(
    commit_transaction(author = "Tester", commit_message = "first load")
  )
  expect_length(msgs, 1)
  expect_match(msgs, "Committed snapshot [0-9]+ \\(Tester\\): first load")
  reported <- as.integer(sub(".*Committed snapshot ([0-9]+).*", "\\1", msgs))
  expect_equal(reported, max(list_table_snapshots()$snapshot_id))

  # Without metadata the line ends after the id
  begin_transaction()
  rows_insert(get_ducklake_table("confirm_t"), data.frame(id = 4L), by = "id")
  expect_message(commit_transaction(), "Committed snapshot [0-9]+\\.")
})

test_that("a commit that changed nothing says so", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  before <- max(list_table_snapshots()$snapshot_id)
  begin_transaction()
  expect_message(commit_transaction(author = "Tester"), "no changes")
  expect_equal(max(list_table_snapshots()$snapshot_id), before)
})

test_that("with_transaction emits a single confirmation", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  msgs <- capture_ducklake_messages(
    with_transaction(
      create_table(data.frame(id = 1:2), "single_t"),
      author = "Tester",
      commit_message = "one line"
    )
  )
  expect_length(msgs, 1)
  expect_match(msgs, "Committed snapshot [0-9]+ \\(Tester\\): one line")
})

test_that("rollback clears the pending confirmation", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:2), "rb_t")
  begin_transaction()
  expect_false(is.null(get_ducklake_env()$txn_snapshot_before))
  rows_insert(get_ducklake_table("rb_t"), data.frame(id = 3L), by = "id")
  expect_message(rollback_transaction(), "rolled back")
  expect_null(get_ducklake_env()$txn_snapshot_before)
  expect_equal(nrow(dplyr::collect(get_ducklake_table("rb_t"))), 2)
})
