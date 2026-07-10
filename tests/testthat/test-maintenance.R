# Tests for the targeted maintenance wrappers: expire_snapshots,
# merge_adjacent_files, cleanup_old_files, delete_orphaned_files, and
# rewrite_data_files.

test_that("maintenance wrappers validate their arguments", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  expect_error(expire_snapshots(), "either")
  expect_error(expire_snapshots(versions = integer(0)), "snapshot ids")
  expect_error(cleanup_old_files(), "older_than")
  expect_error(
    cleanup_old_files(older_than = Sys.time(), cleanup_all = TRUE),
    "both"
  )
  expect_error(delete_orphaned_files(), "older_than")
  expect_error(
    rewrite_data_files(delete_threshold = 2),
    "between 0 and 1"
  )
  expect_error(
    expire_snapshots(ducklake_name = "bad name", older_than = Sys.time()),
    "simple identifier"
  )
})

test_that("merge, expire, and cleanup reclaim small files", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  # Write real Parquet files instead of inlining into the catalog
  suppressMessages(set_inlining_row_limit(0))
  on.exit(suppressMessages(set_inlining_row_limit(10)), add = TRUE)

  create_table(data.frame(id = 1:5), "mnt")
  for (i in 6:8) {
    rows_insert(get_ducklake_table("mnt"), data.frame(id = i), by = "id")
  }
  n_files <- function() {
    length(list.files(lake$temp_dir, pattern = "\\.parquet$", recursive = TRUE))
  }
  expect_equal(n_files(), 4)

  merged <- merge_adjacent_files(table_name = "mnt")
  expect_s3_class(merged, "data.frame")
  expect_equal(sum(merged$files_processed), 4)
  expect_equal(nrow(merged), 1)
  # The merged originals stay on disk until snapshots are expired and cleaned
  expect_equal(n_files(), 5)

  # Dry run lists expirable snapshots without expiring anything
  snaps_before <- nrow(list_table_snapshots())
  preview <- expire_snapshots(older_than = Sys.time() + 60, dry_run = TRUE)
  expect_gt(nrow(preview), 0)
  expect_equal(nrow(list_table_snapshots()), snaps_before)

  expired <- expire_snapshots(older_than = Sys.time() + 60)
  expect_equal(nrow(expired), nrow(preview))

  # Cleanup previews the four pre-merge files, then removes them
  preview_files <- cleanup_old_files(dry_run = TRUE, cleanup_all = TRUE)
  expect_equal(nrow(preview_files), 4)
  expect_equal(n_files(), 5)
  cleanup_old_files(cleanup_all = TRUE)
  expect_equal(n_files(), 1)

  # Data survives the full maintenance cycle
  expect_setequal(dplyr::collect(get_ducklake_table("mnt"))$id, 1:8)
})

test_that("expire_snapshots expires specific versions", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3), "vers")
  rows_insert(get_ducklake_table("vers"), data.frame(id = 4L), by = "id")

  snaps <- list_table_snapshots()
  oldest <- min(snaps$snapshot_id)

  expired <- expire_snapshots(versions = oldest)
  expect_equal(expired$snapshot_id, oldest)
  expect_false(oldest %in% list_table_snapshots()$snapshot_id)
})

test_that("rewrite_data_files rewrites heavily deleted files", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  suppressMessages(set_inlining_row_limit(0))
  on.exit(suppressMessages(set_inlining_row_limit(10)), add = TRUE)

  create_table(data.frame(id = 1:100), "rw")
  rows_delete(get_ducklake_table("rw"), data.frame(id = 1:96), by = "id")

  result <- rewrite_data_files(table_name = "rw", delete_threshold = 0.5)
  expect_gte(sum(result$files_processed), 1)
  expect_setequal(dplyr::collect(get_ducklake_table("rw"))$id, 97:100)
})

test_that("delete_orphaned_files removes only untracked files", {
  skip_if_no_ducklake()
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  raw_dir <- tempfile()
  dir.create(raw_dir, recursive = TRUE)
  temp_dir <- normalizePath(raw_dir)
  lake_name <- paste0("orphlake_", sample.int(.Machine$integer.max, 1))
  attach_ducklake(lake_name, lake_path = temp_dir)
  on.exit({
    tryCatch(detach_ducklake(lake_name), error = function(e) NULL)
    unlink(temp_dir, recursive = TRUE)
  }, add = TRUE)

  suppressMessages(set_inlining_row_limit(0))
  on.exit(suppressMessages(set_inlining_row_limit(10)), add = TRUE)

  create_table(data.frame(id = 1:5), "orph")

  # A healthy lake has no orphans
  expect_equal(
    nrow(delete_orphaned_files(dry_run = TRUE, cleanup_all = TRUE)),
    0
  )

  stray <- file.path(temp_dir, "main", "orph", "stray.parquet")
  writeLines("junk", stray)

  preview <- delete_orphaned_files(dry_run = TRUE, cleanup_all = TRUE)
  expect_equal(nrow(preview), 1)
  expect_true(file.exists(stray))

  delete_orphaned_files(cleanup_all = TRUE)
  expect_false(file.exists(stray))
  expect_equal(nrow(dplyr::collect(get_ducklake_table("orph"))), 5)
})

test_that("doubled slashes in lake_path don't make live files look orphaned", {
  skip_if_no_ducklake()
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  # DuckLake stores DATA_PATH verbatim and compares file paths as exact
  # strings. Before attach_ducklake() normalized the path, a doubled slash
  # (as R's tempdir() produces on macOS) made delete_orphaned_files()
  # classify every live data file as orphaned.
  raw_dir <- tempfile()
  dir.create(raw_dir, recursive = TRUE)
  clean <- normalizePath(raw_dir)
  dirty <- paste0(dirname(clean), "//", basename(clean))
  lake_name <- paste0("slashlake_", sample.int(.Machine$integer.max, 1))
  attach_ducklake(lake_name, lake_path = dirty)
  on.exit({
    tryCatch(detach_ducklake(lake_name), error = function(e) NULL)
    unlink(clean, recursive = TRUE)
  }, add = TRUE)

  suppressMessages(set_inlining_row_limit(0))
  on.exit(suppressMessages(set_inlining_row_limit(10)), add = TRUE)

  create_table(data.frame(id = 1:5), "slashed")

  expect_equal(
    nrow(delete_orphaned_files(dry_run = TRUE, cleanup_all = TRUE)),
    0
  )
  expect_equal(nrow(dplyr::collect(get_ducklake_table("slashed"))), 5)
})

test_that("backup_ducklake rejects remote data paths", {
  expect_error(
    backup_ducklake("some_lake", "s3://bucket/lake", tempdir()),
    "only supports local data paths"
  )
})
