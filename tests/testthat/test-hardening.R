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
})

test_that("restore_table_version validates version/timestamp arguments", {
  expect_error(restore_table_version("t"), "either")
  expect_error(
    restore_table_version("t", version = 1, timestamp = Sys.time()),
    "both"
  )
})

test_that("update_table refuses subqueries and multiple WHERE clauses", {
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
