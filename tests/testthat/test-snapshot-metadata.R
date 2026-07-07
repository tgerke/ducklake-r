# Tests for snapshot metadata via CALL set_commit_message() and direct UPDATE

# --- with_transaction() metadata (uses CALL API under the hood) ---

test_that("with_transaction sets author and commit_message via CALL API", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(mtcars[1:5, ], "meta_call_1"),
    author = "Dr. O'Brien",
    commit_message = "It's a table with 'quoted' values"
  )

  snapshots <- list_table_snapshots("meta_call_1")
  expect_equal(nrow(snapshots), 1)
  expect_equal(snapshots$author, "Dr. O'Brien")
  expect_equal(snapshots$commit_message, "It's a table with 'quoted' values")

  cleanup_temp_ducklake(lake)
})

test_that("with_transaction sets commit_extra_info via CALL API", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(mtcars[1:5, ], "meta_call_extra"),
    author = "Analyst",
    commit_message = "Add mtcars subset",
    commit_extra_info = '{"pipeline": "etl_v2", "run_id": 42}'
  )

  snapshots <- list_table_snapshots("meta_call_extra")
  expect_equal(nrow(snapshots), 1)
  expect_equal(snapshots$author, "Analyst")
  expect_equal(snapshots$commit_message, "Add mtcars subset")
  expect_equal(snapshots$commit_extra_info, '{"pipeline": "etl_v2", "run_id": 42}')

  cleanup_temp_ducklake(lake)
})

# --- commit_transaction() metadata (uses CALL API directly) ---

test_that("commit_transaction sets metadata via CALL API", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  begin_transaction()
  create_table(mtcars[1:3, ], "meta_commit_1")
  commit_transaction(
    author = "CI Bot",
    commit_message = "Automated table creation"
  )

  snapshots <- list_table_snapshots("meta_commit_1")
  expect_equal(nrow(snapshots), 1)
  expect_equal(snapshots$author, "CI Bot")
  expect_equal(snapshots$commit_message, "Automated table creation")

  cleanup_temp_ducklake(lake)
})

# --- set_snapshot_metadata() (retroactive UPDATE on metadata table) ---

test_that("set_snapshot_metadata updates metadata retroactively", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(mtcars[1:5, ], "meta_retro_1"),
    author = "Original",
    commit_message = "first pass"
  )

  set_snapshot_metadata(
    ducklake_name = lake$ducklake_name,
    author = "Corrected Author",
    commit_message = "Corrected message"
  )

  snapshots <- list_table_snapshots("meta_retro_1")
  expect_equal(snapshots$author, "Corrected Author")
  expect_equal(snapshots$commit_message, "Corrected message")

  cleanup_temp_ducklake(lake)
})

test_that("set_snapshot_metadata handles single quotes and special characters", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(mtcars[1:5, ], "meta_retro_2"),
    author = "Test",
    commit_message = "init"
  )

  set_snapshot_metadata(
    ducklake_name = lake$ducklake_name,
    author = "François O'Malley",
    commit_message = "Added 'special' chars: è, ñ, ü"
  )

  snapshots <- list_table_snapshots("meta_retro_2")
  expect_equal(snapshots$author, "François O'Malley")
  expect_equal(snapshots$commit_message, "Added 'special' chars: è, ñ, ü")

  cleanup_temp_ducklake(lake)
})

test_that("set_snapshot_metadata supports commit_extra_info", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(mtcars[1:3, ], "meta_retro_extra")
  )

  set_snapshot_metadata(
    ducklake_name = lake$ducklake_name,
    author = "Reviewer",
    commit_message = "Reviewed",
    commit_extra_info = '{"approved": true}'
  )

  snapshots <- list_table_snapshots("meta_retro_extra")
  expect_equal(snapshots$author, "Reviewer")
  expect_equal(snapshots$commit_message, "Reviewed")
  expect_equal(snapshots$commit_extra_info, '{"approved": true}')

  cleanup_temp_ducklake(lake)
})

test_that("set_snapshot_metadata warns when no metadata provided", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()

  with_transaction(
    create_table(mtcars[1:5, ], "meta_warn")
  )

  expect_warning(set_snapshot_metadata(lake$ducklake_name), "No metadata")

  cleanup_temp_ducklake(lake)
})
