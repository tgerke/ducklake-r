# UNIT TESTS: schema evolution
#
# These tests verify the in-place ALTER TABLE wrappers: add_table_column(),
# drop_table_column(), rename_table_column(), rename_ducklake_table(), and
# set_column_type(). All are metadata-only changes that preserve history.

test_that("add_table_column adds a NULL column and a backfilled default", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, value = c("a", "b")), "test_add_col")

  add_table_column("test_add_col", "note", "VARCHAR")
  add_table_column("test_add_col", "score", "DECIMAL(5,2)", default = 1.5)

  result <- get_ducklake_table("test_add_col") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_true(all(is.na(result$note)))
  # DuckLake defaults apply to existing rows, not just future inserts
  expect_equal(result$score, c(1.5, 1.5))

  cleanup_temp_ducklake(lake)
})

test_that("added columns are absent from earlier snapshots", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2), "test_add_history")
  before <- max(list_table_snapshots()$snapshot_id)

  add_table_column("test_add_history", "extra", "INTEGER")

  old <- get_ducklake_table_version("test_add_history", before) |>
    dplyr::collect()
  current <- get_ducklake_table("test_add_history") |> dplyr::collect()
  expect_named(old, "id")
  expect_named(current, c("id", "extra"))

  cleanup_temp_ducklake(lake)
})

test_that("drop_table_column removes the column but time travel still shows it", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, scratch = c("x", "y")), "test_drop_col")
  before <- max(list_table_snapshots()$snapshot_id)

  drop_table_column("test_drop_col", "scratch")

  current <- get_ducklake_table("test_drop_col") |> dplyr::collect()
  expect_named(current, "id")
  old <- get_ducklake_table_version("test_drop_col", before) |>
    dplyr::collect()
  expect_named(old, c("id", "scratch"))
  expect_equal(old$scratch, c("x", "y"))

  cleanup_temp_ducklake(lake)
})

test_that("rename_table_column round trip preserves data", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, old_name = c("a", "b")), "test_rename_col")

  rename_table_column("test_rename_col", from = "old_name", to = "new_name")

  result <- get_ducklake_table("test_rename_col") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_named(result, c("id", "new_name"))
  expect_equal(result$new_name, c("a", "b"))

  cleanup_temp_ducklake(lake)
})

test_that("rename_ducklake_table moves the table and history stays reachable", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2), "test_rename_tbl")
  before <- max(list_table_snapshots()$snapshot_id)

  rename_ducklake_table("test_rename_tbl", "test_renamed")

  result <- get_ducklake_table("test_renamed") |> dplyr::collect()
  expect_equal(nrow(result), 2)
  # current reads under the old name fail
  expect_error(
    get_ducklake_table("test_rename_tbl") |> dplyr::collect()
  )
  # pre-rename snapshots stay associated with the old name
  old <- get_ducklake_table_version("test_rename_tbl", before) |>
    dplyr::collect()
  expect_equal(nrow(old), 2)

  cleanup_temp_ducklake(lake)
})

test_that("set_column_type performs a widening promotion", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2), "test_widen")

  set_column_type("test_widen", "id", "BIGINT")

  type <- DBI::dbGetQuery(
    get_ducklake_connection(),
    "SELECT data_type FROM duckdb_columns()
     WHERE table_name = 'test_widen' AND column_name = 'id'"
  )$data_type
  expect_equal(type, "BIGINT")

  cleanup_temp_ducklake(lake)
})

test_that("set_column_type surfaces the narrowing workaround on lossy changes", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2), "test_narrow")
  set_column_type("test_narrow", "id", "BIGINT")

  expect_error(
    set_column_type("test_narrow", "id", "INTEGER"),
    "widening type changes"
  )

  cleanup_temp_ducklake(lake)
})

test_that("derived columns fill in place via add_table_column + ducklake_exec", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:3, hp = c(90, 210, 150)), "test_derive")

  add_table_column("test_derive", "high_hp", "VARCHAR")
  get_ducklake_table("test_derive") |>
    dplyr::mutate(high_hp = dplyr::if_else(hp > 200, "Y", "N")) |>
    ducklake_exec()

  result <- get_ducklake_table("test_derive") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$high_hp, c("N", "Y", "N"))

  cleanup_temp_ducklake(lake)
})

test_that("the type guard rejects injection and accepts real types", {
  skip_if_not_installed("duckdb")

  expect_error(
    check_column_type("INTEGER; DROP TABLE x"),
    "plain SQL type"
  )
  expect_error(check_column_type("INTEGER'--"), "plain SQL type")
  expect_error(check_column_type(c("A", "B")), "single SQL type")
  expect_error(check_column_type(NA_character_), "single SQL type")

  expect_invisible(check_column_type("INTEGER"))
  expect_invisible(check_column_type("DECIMAL(10,2)"))
  expect_invisible(check_column_type("TIMESTAMP WITH TIME ZONE"))
  expect_invisible(check_column_type("INTEGER[]"))
})

test_that("render_sql_literal covers the DEFAULT value types", {
  expect_equal(render_sql_literal("new"), "'new'")
  expect_equal(render_sql_literal("it's"), "'it''s'")
  expect_equal(render_sql_literal(1.5), "1.5")
  expect_equal(render_sql_literal(TRUE), "TRUE")
  expect_equal(render_sql_literal(NA), "NULL")
  expect_equal(
    render_sql_literal(as.Date("2026-01-15")),
    "DATE '2026-01-15'"
  )
  expect_match(
    render_sql_literal(as.POSIXct("2026-01-15 10:00:00", tz = "UTC")),
    "^TIMESTAMP '2026-01-15 10:00:00"
  )
  expect_error(render_sql_literal(list(1)), "SQL literal")
})
