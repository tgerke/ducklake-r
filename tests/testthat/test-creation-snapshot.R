# UNIT TESTS: the creation snapshot
#
# DuckLake writes snapshot 0 when it creates a lake, with no author or
# message. attach_ducklake() labels it for a lake it creates, and
# set_snapshot_metadata(snapshot_id = 0) labels it after the fact.

# A lake attached by hand, so the attach arguments are what is under test
new_lake_spec <- function() {
  skip_if_no_ducklake()
  dir <- tempfile("creation_")
  dir.create(dir, recursive = TRUE)
  list(name = paste0("crlake_", sample.int(.Machine$integer.max, 1)), dir = dir)
}

cleanup_lake_spec <- function(lake) {
  tryCatch(detach_ducklake(lake$name), error = function(e) NULL)
  unlink(lake$dir, recursive = TRUE)
}

creation_row <- function() {
  snaps <- list_table_snapshots()
  snaps[snaps$snapshot_id == 0, , drop = FALSE]
}

test_that("attach_ducklake() labels snapshot 0 of a lake it creates", {
  skip_if_not_installed("dplyr")
  lake <- new_lake_spec()
  on.exit(cleanup_lake_spec(lake), add = TRUE)

  expect_no_message(
    attach_ducklake(lake$name, lake_path = lake$dir),
    class = "ducklake_message"
  )

  snaps <- list_table_snapshots()
  expect_equal(nrow(snaps), 1)
  expect_equal(snaps$snapshot_id, 0)
  expect_true(is.na(snaps$author))
  expect_equal(snaps$commit_message, "Create lake")
  expect_true(is.na(snaps$commit_extra_info))

  # The raw metadata row agrees
  row <- get_metadata_table("ducklake_snapshot_changes") |> dplyr::collect()
  expect_equal(row$snapshot_id, 0)
  expect_equal(row$commit_message, "Create lake")
})

test_that("the author is recorded when given, and commit_message = NULL opts out", {
  lake <- new_lake_spec()
  on.exit(cleanup_lake_spec(lake), add = TRUE)

  attach_ducklake(lake$name, lake_path = lake$dir, author = "Data Engineer")
  snaps <- list_table_snapshots()
  expect_equal(snaps$author, "Data Engineer")
  expect_equal(snaps$commit_message, "Create lake")

  other <- new_lake_spec()
  on.exit(cleanup_lake_spec(other), add = TRUE)
  attach_ducklake(other$name, lake_path = other$dir, commit_message = NULL)
  snaps <- list_table_snapshots()
  expect_equal(nrow(snaps), 1)
  expect_true(is.na(snaps$author))
  expect_true(is.na(snaps$commit_message))
})

test_that("a lake that already exists is left alone", {
  lake <- new_lake_spec()
  on.exit(cleanup_lake_spec(lake), add = TRUE)

  # Created without a label, then opened again with one: still blank
  attach_ducklake(lake$name, lake_path = lake$dir, commit_message = NULL)
  detach_ducklake(lake$name)
  attach_ducklake(lake$name, lake_path = lake$dir, author = "Someone")
  snaps <- list_table_snapshots()
  expect_true(is.na(snaps$author))
  expect_true(is.na(snaps$commit_message))

  # create = FALSE opens an existing lake, so it never labels either
  detach_ducklake(lake$name)
  attach_ducklake(lake$name, lake_path = lake$dir, create = FALSE, author = "Someone")
  expect_true(is.na(list_table_snapshots()$commit_message))

  # A labeled lake keeps its first label
  labeled <- new_lake_spec()
  on.exit(cleanup_lake_spec(labeled), add = TRUE)
  attach_ducklake(labeled$name, lake_path = labeled$dir, author = "First")
  detach_ducklake(labeled$name)
  attach_ducklake(labeled$name, lake_path = labeled$dir, author = "Second")
  expect_equal(list_table_snapshots()$author, "First")

  # And so does a second attach while it is still attached
  attach_ducklake(labeled$name, lake_path = labeled$dir, author = "Third")
  expect_equal(list_table_snapshots()$author, "First")
})

test_that("a read-only attach writes nothing and does not warn", {
  lake <- new_lake_spec()
  on.exit(cleanup_lake_spec(lake), add = TRUE)

  # DuckDB cannot create a lake read-only, so the lake exists blank first
  attach_ducklake(lake$name, lake_path = lake$dir, commit_message = NULL)
  detach_ducklake(lake$name)
  expect_no_warning(
    attach_ducklake(lake$name, lake_path = lake$dir, read_only = TRUE, author = "x")
  )
  snaps <- list_table_snapshots()
  expect_true(is.na(snaps$author))
  expect_true(is.na(snaps$commit_message))
})

test_that("a snapshot-pinned attach of a new lake writes nothing", {
  lake <- new_lake_spec()
  on.exit(cleanup_lake_spec(lake), add = TRUE)

  # The lake is read-only but its metadata catalog is writable: only the
  # guard in attach_ducklake() stops the label
  attach_ducklake(lake$name, lake_path = lake$dir, snapshot_version = 0, author = "x")
  snaps <- list_table_snapshots()
  expect_equal(nrow(snaps), 1)
  expect_true(is.na(snaps$author))
  expect_true(is.na(snaps$commit_message))
})

test_that("an attach inside an open transaction skips the label and leaves writes working", {
  lake <- new_lake_spec()
  on.exit(cleanup_lake_spec(lake), add = TRUE)

  begin_transaction()
  attach_ducklake(lake$name, lake_path = lake$dir, author = "x")
  create_table(data.frame(id = 1), "t")
  expect_message(commit_transaction(), "Committed snapshot 1")

  zero <- creation_row()
  expect_true(is.na(zero$author))
  expect_true(is.na(zero$commit_message))

  # Labeled after the fact
  suppressMessages(set_snapshot_metadata(
    lake$name, author = "x", commit_message = "Create lake", snapshot_id = 0
  ))
  zero <- creation_row()
  expect_equal(zero$author, "x")
  expect_equal(zero$commit_message, "Create lake")
})

test_that("the SQLite backend is labeled and the label persists", {
  skip_if_no_ducklake()

  dir <- tempfile("creation_sqlite_")
  dir.create(file.path(dir, "data"), recursive = TRUE)
  name <- paste0("crsqlite_", sample.int(.Machine$integer.max, 1))
  on.exit({
    tryCatch(detach_ducklake(name), error = function(e) NULL)
    unlink(dir, recursive = TRUE)
  }, add = TRUE)
  catalog <- file.path(dir, "metadata.sqlite")
  attach_sqlite <- function(author) {
    tryCatch(
      attach_ducklake(
        name, lake_path = file.path(dir, "data"), backend = "sqlite",
        catalog_connection_string = catalog, author = author
      ),
      error = function(e) {
        if (grepl("sqlite", conditionMessage(e), ignore.case = TRUE)) {
          skip(paste("SQLite backend unavailable:", conditionMessage(e)))
        }
        stop(e)
      }
    )
  }

  attach_sqlite("Data Engineer")
  snaps <- list_table_snapshots()
  expect_equal(snaps$author, "Data Engineer")
  expect_equal(snaps$commit_message, "Create lake")

  detach_ducklake(name)
  attach_sqlite("Someone Else")
  snaps <- list_table_snapshots()
  expect_equal(snaps$author, "Data Engineer")
  expect_equal(snaps$commit_message, "Create lake")
})

test_that("metadata_schema is honored end to end", {
  skip_if_not_installed("dplyr")
  lake <- new_lake_spec()
  on.exit(cleanup_lake_spec(lake), add = TRUE)

  attach_ducklake(
    lake$name, lake_path = lake$dir, metadata_schema = "meta", author = "Data Engineer"
  )
  expect_equal(get_ducklake_env()$lakes[[lake$name]]$metadata_schema, "meta")
  expect_match(ducklake:::metadata_prefix(lake$name), '[.]"?meta"?$')

  snaps <- list_table_snapshots()
  expect_equal(snaps$author, "Data Engineer")
  expect_equal(snaps$commit_message, "Create lake")

  # The other metadata writers and readers find the schema too
  expect_no_warning(
    suppressMessages(set_snapshot_metadata(lake$name, commit_extra_info = "x"))
  )
  expect_equal(list_table_snapshots()$commit_extra_info, "x")
  row <- get_metadata_table("ducklake_snapshot_changes") |> dplyr::collect()
  expect_equal(row$commit_extra_info, "x")
  create_table(data.frame(id = 1:2), "t")
  expect_no_warning(get_table_partitions("t"))

  # A second attach without the argument keeps the registered schema
  attach_ducklake(lake$name, lake_path = lake$dir)
  expect_equal(get_ducklake_env()$lakes[[lake$name]]$metadata_schema, "meta")
})

test_that("the label stays out of a table's history", {
  lake <- new_lake_spec()
  on.exit(cleanup_lake_spec(lake), add = TRUE)

  attach_ducklake(lake$name, lake_path = lake$dir)
  create_table(data.frame(id = 1:2), "t")
  expect_false(0 %in% list_table_snapshots("t")$snapshot_id)
  expect_true(0 %in% list_table_snapshots()$snapshot_id)
})

test_that("author and commit_message are validated before anything is created", {
  lake <- new_lake_spec()
  on.exit(cleanup_lake_spec(lake), add = TRUE)

  expect_error(
    attach_ducklake(lake$name, lake_path = lake$dir, author = c("a", "b")),
    "single non-empty string"
  )
  expect_error(
    attach_ducklake(lake$name, lake_path = lake$dir, author = ""),
    "single non-empty string"
  )
  expect_error(
    attach_ducklake(lake$name, lake_path = lake$dir, commit_message = 1),
    "single non-empty string"
  )
  expect_false(file.exists(file.path(lake$dir, paste0(lake$name, ".ducklake"))))
})

test_that("set_snapshot_metadata() on a fresh lake targets snapshot 0", {
  lake <- new_lake_spec()
  on.exit(cleanup_lake_spec(lake), add = TRUE)
  attach_ducklake(lake$name, lake_path = lake$dir)

  expect_message(set_snapshot_metadata(lake$name, author = "Late Author"), "snapshot 0")
  expect_equal(list_table_snapshots()$author, "Late Author")
  expect_error(set_snapshot_metadata(lake$name, commit_message = "x"), "already has")
  suppressMessages(
    set_snapshot_metadata(lake$name, commit_message = "Create the lake", overwrite = TRUE)
  )
  expect_equal(list_table_snapshots()$commit_message, "Create the lake")
})

test_that("snapshot_id picks the snapshot to label", {
  lake <- new_lake_spec()
  on.exit(cleanup_lake_spec(lake), add = TRUE)
  attach_ducklake(lake$name, lake_path = lake$dir)
  create_table(data.frame(id = 1), "one")
  create_table(data.frame(id = 2), "two")

  expect_message(
    set_snapshot_metadata(
      lake$name, author = "A", commit_message = "first table", snapshot_id = 1
    ),
    "snapshot 1"
  )
  snaps <- list_table_snapshots()
  expect_equal(snaps$author[snaps$snapshot_id == 1], "A")
  expect_true(is.na(snaps$author[snaps$snapshot_id == 2]))
  expect_equal(snaps$commit_message[snaps$snapshot_id == 0], "Create lake")

  expect_error(
    set_snapshot_metadata(lake$name, commit_message = "x", snapshot_id = 0),
    "already has"
  )
  expect_error(
    set_snapshot_metadata(lake$name, author = "x", snapshot_id = 99),
    "does not exist"
  )
  expect_error(
    set_snapshot_metadata(lake$name, author = "x", snapshot_id = "a"),
    "whole number"
  )
  expect_error(
    set_snapshot_metadata(lake$name, author = "x", snapshot_id = -1),
    "whole number"
  )
  expect_error(
    set_snapshot_metadata(lake$name, author = "x", snapshot_id = 1.5),
    "whole number"
  )

  # The default is still the latest snapshot
  suppressMessages(set_snapshot_metadata(lake$name, author = "B"))
  snaps <- list_table_snapshots()
  expect_equal(snaps$author[snaps$snapshot_id == 2], "B")
})
