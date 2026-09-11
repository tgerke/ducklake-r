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
  expect_false(is.null(get_ducklake_env()$txn_committed_before))
  rows_insert(get_ducklake_table("rb_t"), data.frame(id = 3L), by = "id")
  expect_message(rollback_transaction(), "rolled back")
  expect_null(get_ducklake_env()$txn_committed_before)
  expect_equal(nrow(dplyr::collect(get_ducklake_table("rb_t"))), 2)
})

test_that("the confirmation names this connection's commit, not a neighbor's", {
  skip_if_no_ducklake()
  skip_if_not_installed("dplyr")

  # Two DuckDB connections on one SQLite catalog: the package connection
  # and a raw neighbor
  dir <- gsub("/+", "/", tempfile("neighbor_"))
  dir.create(file.path(dir, "data"), recursive = TRUE)
  catalog <- file.path(dir, "metadata.sqlite")
  name <- paste0("nblake_", sample.int(.Machine$integer.max, 1))
  on.exit({
    tryCatch(detach_ducklake(name), error = function(e) NULL)
    unlink(dir, recursive = TRUE)
  }, add = TRUE)
  tryCatch(
    attach_ducklake(
      name, lake_path = file.path(dir, "data"), backend = "sqlite",
      catalog_connection_string = catalog
    ),
    error = function(e) {
      if (grepl("sqlite", conditionMessage(e), ignore.case = TRUE)) {
        skip(paste("SQLite backend unavailable:", conditionMessage(e)))
      }
      stop(e)
    }
  )
  create_table(data.frame(id = 1), "mine")

  neighbor <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(neighbor, shutdown = TRUE), add = TRUE, after = FALSE)
  DBI::dbExecute(neighbor, "LOAD ducklake")
  DBI::dbExecute(neighbor, "LOAD sqlite")
  DBI::dbExecute(neighbor, sprintf(
    "ATTACH 'ducklake:sqlite:%s' AS other (DATA_PATH '%s')",
    catalog, get_ducklake_info(name)$data_path
  ))

  # The neighbor commits after begin_transaction() and before this
  # transaction touches the lake (a SQLite catalog is locked from then
  # on): the confirmation names this connection's snapshot, not the
  # neighbor's
  begin_transaction()
  DBI::dbExecute(neighbor, "CREATE TABLE other.theirs AS SELECT 1 AS id")
  create_table(data.frame(id = 2), "mine2")
  msgs <- capture_ducklake_messages(
    commit_transaction(author = "Me", commit_message = "mine2")
  )
  expect_match(msgs, "Committed snapshot [0-9]+ \\(Me\\): mine2")
  reported <- as.integer(sub(".*Committed snapshot ([0-9]+).*", "\\1", msgs))
  expect_equal(reported, list_table_snapshots("mine2")$snapshot_id)
  expect_false(reported %in% list_table_snapshots("theirs")$snapshot_id)

  # A transaction that changed nothing is not credited with the neighbor's
  # commit; the lake position it names is the neighbor's snapshot
  begin_transaction()
  DBI::dbExecute(neighbor, "INSERT INTO other.theirs VALUES (2)")
  msgs <- capture_ducklake_messages(commit_transaction())
  expect_match(msgs, "no changes")
  expect_match(msgs, paste0("snapshot ", max(list_table_snapshots()$snapshot_id), "\\."))
})
