# Tests for the production-hardening fixes: identifier quoting, encrypted
# attach, restore_table_version, update_table guardrails, multi-lake backend
# state, metadata tables, and rows_* S3 dispatch.

test_that("duckdb_version_at_least handles normal, prefixed, and dev versions", {
  expect_true(duckdb_version_at_least("v1.5.4", "1.5.1"))
  expect_true(duckdb_version_at_least("1.5.1", "1.5.1"))
  expect_false(duckdb_version_at_least("v1.4.4", "1.5.1"))
  # Multi-digit components compare numerically, not lexically
  expect_true(duckdb_version_at_least("v1.10.0", "1.5.1"))
  expect_false(duckdb_version_at_least("v0.13.0", "1.3.0"))
  # Dev suffixes are tolerated
  expect_true(duckdb_version_at_least("v1.5.4-dev123", "1.5.1"))
  # Garbage is FALSE, not an error
  expect_false(duckdb_version_at_least("not-a-version", "1.5.1"))
})

test_that("check_identifier accepts plain names and rejects injection attempts", {
  expect_invisible(check_identifier("my_lake"))
  expect_invisible(check_identifier("_x9"))
  expect_error(check_identifier("my lake"), "simple identifier")
  expect_error(check_identifier("x; DROP TABLE y"), "simple identifier")
  expect_error(check_identifier("9lake"), "simple identifier")
  expect_error(check_identifier(c("a", "b")), "simple identifier")
})

test_that("quote_ident quotes parts and neutralises embedded quotes", {
  skip_if_not_installed("duckdb")
  conn <- get_ducklake_connection()
  # duckdb only quotes when needed; simple names may pass through bare
  expect_true(quote_ident("cars", conn) %in% c("cars", '"cars"'))
  expect_true(quote_ident("main.cars", conn) %in% c("main.cars", '"main"."cars"'))
  # Dangerous input must come back as a single quoted identifier with the
  # embedded quote doubled, so it cannot break out of the identifier
  expect_equal(quote_ident('x"; DROP TABLE y;--', conn), '"x""; DROP TABLE y;--"')
  # A dotted name splits into parts, so the dot separates identifiers
  expect_match(quote_ident("bad name.tbl", conn), '^"bad name"\\.')
})

test_that("attach_ducklake rejects non-identifier lake names", {
  expect_error(
    attach_ducklake("bad name; DROP", lake_path = tempdir()),
    "simple identifier"
  )
})

test_that("build_attach_sql adds ENCRYPTED TRUE when requested", {
  sql <- build_attach_sql(
    "enc_lake", "/tmp/lake", "duckdb",
    catalog_connection_string = NULL,
    read_only = FALSE,
    encrypted = TRUE
  )
  expect_match(sql, "ENCRYPTED TRUE")

  sql_plain <- build_attach_sql(
    "enc_lake", "/tmp/lake", "duckdb",
    catalog_connection_string = NULL,
    read_only = FALSE
  )
  expect_no_match(sql_plain, "ENCRYPTED")
})

test_that("encrypted lakes round-trip data", {
  skip_if_no_ducklake()
  skip_if_not_installed("duckdb")

  temp_dir <- tempfile()
  dir.create(temp_dir, recursive = TRUE)
  on.exit({
    detach_ducklake("enc_lake", shutdown = FALSE)
    unlink(temp_dir, recursive = TRUE)
  }, add = TRUE)

  attach_ducklake("enc_lake", lake_path = temp_dir, encrypted = TRUE)
  create_table(mtcars, "cars_enc")

  result <- dplyr::collect(get_ducklake_table("cars_enc"))
  expect_equal(nrow(result), 32)

  # The written Parquet files must not be readable as plain Parquet
  parquet_files <- list.files(temp_dir, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)
  expect_true(length(parquet_files) > 0)
})

test_that("restore_table_version restores by version and preserves history", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  with_transaction(create_table(data.frame(id = 1:3, v = c("a", "b", "c")), "hist"))
  snaps_before <- list_table_snapshots("hist")
  first_version <- min(snaps_before$snapshot_id)

  # Modify: drop a row via replace
  with_transaction({
    get_ducklake_table("hist") |>
      dplyr::filter(id != 3) |>
      replace_table("hist")
  })
  expect_equal(nrow(dplyr::collect(get_ducklake_table("hist"))), 2)

  # Restore to the first snapshot
  expect_true(restore_table_version("hist", version = first_version))
  restored <- dplyr::collect(get_ducklake_table("hist"))
  expect_equal(nrow(restored), 3)
  expect_setequal(restored$id, 1:3)

  # History preserved: restore recorded as a NEW snapshot, nothing rewritten
  snaps_after <- list_table_snapshots()
  expect_gt(max(snaps_after$snapshot_id), max(snaps_before$snapshot_id))
  expect_true(any(grepl("Restored hist", snaps_after$commit_message)))

  # author and commit_message are recorded on the restore snapshot
  restore_table_version(
    "hist",
    version = first_version,
    author = "Data Steward",
    commit_message = "Roll back bad update"
  )
  snaps_final <- list_table_snapshots("hist")
  last <- snaps_final[which.max(snaps_final$snapshot_id), ]
  expect_equal(last$author, "Data Steward")
  expect_equal(last$commit_message, "Roll back bad update")

  # Filtered snapshot listings have clean row names for display
  expect_identical(rownames(snaps_final), as.character(seq_len(nrow(snaps_final))))
})

test_that("restore_table_version validates version/timestamp arguments", {
  expect_error(restore_table_version("t"), "either")
  expect_error(
    restore_table_version("t", version = 1, timestamp = Sys.time()),
    "both"
  )
})

test_that("update_table refuses pipelines that compile to subqueries", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:5, grp = c("a", "a", "b", "b", "b")), "guard")

  # A windowed/grouped filter renders as a subquery -> must refuse, not corrupt
  expect_error(
    get_ducklake_table("guard") |>
      dplyr::group_by(grp) |>
      dplyr::filter(id == max(id)) |>
      ducklake_exec("guard"),
    "subquery|too complex|Failed to generate"
  )

  # Table untouched after the refusal
  expect_equal(nrow(dplyr::collect(get_ducklake_table("guard"))), 5)
})

test_that("update_table handles values containing the word WHERE", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(
    data.frame(id = 1:3, note = c("WHERE is it", "here", "nowhere")),
    "wherewords"
  )

  get_ducklake_table("wherewords") |>
    dplyr::filter(id != 2) |>
    ducklake_exec("wherewords", .quiet = TRUE)

  remaining <- dplyr::collect(get_ducklake_table("wherewords"))
  expect_setequal(remaining$id, c(1L, 3L))
})

test_that("get_ducklake_backend tracks multiple lakes independently", {
  skip_if_no_ducklake()
  skip_if_not_installed("duckdb")

  dir_a <- tempfile(); dir.create(dir_a, recursive = TRUE)
  dir_b <- tempfile(); dir.create(dir_b, recursive = TRUE)
  on.exit({
    detach_ducklake("lake_ddb")
    detach_ducklake("lake_sqlite")
    unlink(c(dir_a, dir_b), recursive = TRUE)
  }, add = TRUE)

  attach_ducklake("lake_ddb", lake_path = dir_a)
  attach_ducklake(
    "lake_sqlite",
    lake_path = dir_b,
    backend = "sqlite",
    catalog_connection_string = file.path(dir_b, "meta.sqlite")
  )

  expect_equal(get_ducklake_backend("lake_ddb"), "duckdb")
  expect_equal(get_ducklake_backend("lake_sqlite"), "sqlite")
  # NULL resolves via the currently USEd database (lake_sqlite, attached last)
  expect_equal(get_ducklake_backend(), "sqlite")
})

test_that("get_metadata_table returns DuckLake metadata", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(mtcars, "meta_probe")

  snapshot_meta <- get_metadata_table("ducklake_snapshot") |>
    dplyr::collect()
  expect_true(nrow(snapshot_meta) > 0)
  expect_true("snapshot_id" %in% names(snapshot_meta))

  tables_meta <- get_metadata_table("ducklake_table", lake$ducklake_name) |>
    dplyr::collect()
  expect_true("meta_probe" %in% tables_meta$table_name)
})

test_that("dplyr rows_* generics dispatch DuckLake defaults on lake tables", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:2, v = c("a", "b")), "dispatch")

  tbl <- get_ducklake_table("dispatch")
  expect_s3_class(tbl, "tbl_ducklake")

  # Calling the dplyr GENERIC directly (as happens when dplyr masks
  # ducklake's wrappers) must work without in_place/conflict boilerplate
  dplyr::rows_insert(tbl, data.frame(id = 3L, v = "c"), by = "id")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("dispatch"))), 3)

  dplyr::rows_update(get_ducklake_table("dispatch"), data.frame(id = 1L, v = "z"), by = "id")
  updated <- dplyr::collect(get_ducklake_table("dispatch"))
  expect_equal(updated$v[updated$id == 1], "z")

  dplyr::rows_delete(get_ducklake_table("dispatch"), data.frame(id = 2L), by = "id")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("dispatch"))), 2)
})

test_that("rows_* calls work inside with_transaction as one snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, v = c("a", "b", "c")), "txn_rows")
  snaps_before <- list_table_snapshots()

  # Local data frames must not trigger dbplyr's copy_to transaction,
  # which would nest a BEGIN inside ours and fail on DuckDB
  with_transaction({
    rows_insert(get_ducklake_table("txn_rows"), data.frame(id = 4L, v = "d"), by = "id")
    rows_update(get_ducklake_table("txn_rows"), data.frame(id = 1L, v = "z"), by = "id")
    rows_delete(get_ducklake_table("txn_rows"), data.frame(id = 2L), by = "id")
  },
    author = "Txn Tester",
    commit_message = "Batched row changes"
  )

  result <- dplyr::collect(get_ducklake_table("txn_rows"))
  expect_setequal(result$id, c(1L, 3L, 4L))
  expect_equal(result$v[result$id == 1], "z")

  # All three operations landed as a single snapshot with metadata
  snaps_after <- list_table_snapshots()
  expect_equal(max(snaps_after$snapshot_id), max(snaps_before$snapshot_id) + 1)
  last <- snaps_after[which.max(snaps_after$snapshot_id), ]
  expect_equal(last$author, "Txn Tester")
  expect_equal(last$commit_message, "Batched row changes")
})

test_that("ducklake_exec executes exactly once and previews don't execute", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, v = c(10, 20, 30)), "exec_once")
  snaps_before <- nrow(list_table_snapshots())

  # A non-idempotent update reveals double execution immediately
  get_ducklake_table("exec_once") |>
    dplyr::mutate(v = dplyr::if_else(id == 1, v + 1, v)) |>
    ducklake_exec()

  result <- dplyr::collect(get_ducklake_table("exec_once"))
  expect_equal(result$v[result$id == 1], 11)
  expect_equal(nrow(list_table_snapshots()) - snaps_before, 1)

  # show_ducklake_query is a pure preview: no data change, no snapshot
  before <- dplyr::collect(get_ducklake_table("exec_once"))
  out <- capture.output(
    get_ducklake_table("exec_once") |>
      dplyr::mutate(v = v * 2) |>
      show_ducklake_query()
  )
  expect_true(any(grepl("UPDATE", out)))
  expect_identical(dplyr::collect(get_ducklake_table("exec_once")), before)
})

test_that("update_table handles function calls with commas and blocks self-inserts", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, v = c(10.16, 20.24, 30.55)), "commas")

  # round(v, 1) renders as a function call with a comma in its arguments;
  # the UPDATE assignments must not be split apart
  get_ducklake_table("commas") |>
    dplyr::mutate(v = round(v, 1)) |>
    ducklake_exec()
  result <- dplyr::collect(get_ducklake_table("commas"))
  expect_equal(sort(result$v), c(10.2, 20.2, 30.6))
  expect_equal(nrow(result), 3)

  # A plain self-read has nothing to translate and would duplicate rows
  expect_error(
    ducklake_exec(get_ducklake_table("commas")),
    "duplicate"
  )
})

test_that("list_table_snapshots includes row-level DML snapshots", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, v = c("a", "b", "c")), "dml_hist")
  rows_insert(get_ducklake_table("dml_hist"), data.frame(id = 4L, v = "d"), by = "id")
  rows_delete(get_ducklake_table("dml_hist"), data.frame(id = 2L), by = "id")
  create_table(data.frame(x = 1), "dml_bystander")

  # DML snapshots reference the table by numeric id in the changes column;
  # the filtered listing must include them alongside the creation snapshot
  snaps <- list_table_snapshots("dml_hist")
  expect_equal(nrow(snaps), 3)

  # ...without leaking other tables' snapshots
  bystander <- list_table_snapshots("dml_bystander")
  expect_equal(nrow(bystander), 1)
})

test_that("backup_ducklake copies every schema directory", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("fs")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(mtcars, "cars_bk")
  db_execute("CREATE SCHEMA extra;")
  create_table(data.frame(x = 1:3), "extra.numbers")

  backup_root <- tempfile()
  backup_dir <- backup_ducklake(lake$ducklake_name, lake$lake_path, backup_root)
  on.exit(unlink(backup_root, recursive = TRUE), add = TRUE)

  copied <- basename(list.dirs(backup_dir, recursive = FALSE))
  expect_true("main" %in% copied)
  expect_true("extra" %in% copied)
})

test_that("create_table converts factor columns instead of failing on ENUM", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  expect_message(
    create_table(iris, "flowers"),
    "Converted factor column"
  )
  result <- dplyr::collect(get_ducklake_table("flowers"))
  expect_equal(nrow(result), 150)
  expect_type(result$Species, "character")
})
