# UNIT TESTS: the ducklake.author option
#
# A session-wide default author for the snapshots the package labels. An
# explicit author wins, set_snapshot_metadata() does not read it, and a
# malformed value is an error.

test_that("commit points record the session author", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  old <- options(ducklake.author = "Session Author")
  on.exit(options(old), add = TRUE)

  begin_transaction()
  create_table(data.frame(id = 1:2), "opt_t")
  expect_message(
    commit_transaction(commit_message = "load"),
    "Committed snapshot [0-9]+ \\(Session Author\\): load"
  )
  expect_equal(list_table_snapshots("opt_t")$author, "Session Author")

  # An explicit author wins
  with_transaction(create_table(data.frame(id = 1), "opt_u"), author = "Named")
  expect_equal(list_table_snapshots("opt_u")$author, "Named")

  # with_transaction() and restore_table_version() inherit it
  with_transaction(
    rows_insert(get_ducklake_table("opt_t"), data.frame(id = 3L), by = "id")
  )
  snaps <- list_table_snapshots("opt_t")
  expect_equal(snaps$author[which.max(snaps$snapshot_id)], "Session Author")

  restore_table_version("opt_t", version = min(snaps$snapshot_id))
  snaps <- list_table_snapshots("opt_t")
  expect_equal(snaps$author[which.max(snaps$snapshot_id)], "Session Author")
})

test_that("attach_ducklake() records the session author on the creation snapshot", {
  skip_if_not_installed("duckdb")

  old <- options(ducklake.author = "Session Author")
  on.exit(options(old), add = TRUE)
  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  snaps <- list_table_snapshots()
  expect_equal(snaps$author, "Session Author")
  expect_equal(snaps$commit_message, "Create lake")

  # The argument wins over the option
  other <- create_temp_ducklake(author = "Named")
  on.exit(cleanup_temp_ducklake(other), add = TRUE)
  expect_equal(list_table_snapshots()$author, "Named")
})

test_that("set_snapshot_metadata() and autocommit writes do not read the option", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  old <- options(ducklake.author = "Session Author")
  on.exit(options(old), add = TRUE)

  # A write outside a transaction records no metadata, option or not
  create_table(data.frame(id = 1), "opt_meta")
  expect_true(is.na(list_table_snapshots("opt_meta")$author))

  suppressMessages(
    set_snapshot_metadata(lake$ducklake_name, commit_message = "labeled later")
  )
  snaps <- list_table_snapshots("opt_meta")
  expect_true(is.na(snaps$author))
  expect_equal(snaps$commit_message, "labeled later")
})

test_that("a malformed ducklake.author option is an error", {
  for (bad in list(1, "", c("a", "b"), NA_character_)) {
    old <- options(ducklake.author = bad)
    expect_error(ducklake:::resolve_author(), "ducklake.author")
    options(old)
  }

  # An explicit author is validated the same way, and wins over the option
  expect_error(ducklake:::resolve_author(""), "single non-empty string")
  old <- options(ducklake.author = "Session Author")
  on.exit(options(old), add = TRUE)
  expect_equal(ducklake:::resolve_author("Named"), "Named")
  expect_equal(ducklake:::resolve_author(), "Session Author")
  options(ducklake.author = NULL)
  expect_null(ducklake:::resolve_author())
})
