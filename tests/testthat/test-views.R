# UNIT TESTS: views
#
# These tests verify create_view() and drop_view(), including persistence
# across re-attach and composition with further dplyr verbs.

test_that("create_view stores a pipeline and reads match running it", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(
    data.frame(id = 1:4, amount = c(10, 250, 40, 300)),
    "test_view_base"
  )

  pipeline <- get_ducklake_table("test_view_base") |>
    dplyr::filter(amount > 100) |>
    dplyr::select(id, amount)
  create_view(pipeline, "v_large")

  via_view <- get_ducklake_table("v_large") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  direct <- pipeline |> dplyr::arrange(id) |> dplyr::collect()
  expect_equal(via_view, direct)

  # views read current data: a new qualifying row appears immediately
  rows_insert(
    get_ducklake_table("test_view_base"),
    data.frame(id = 5L, amount = 500),
    by = "id"
  )
  expect_equal(
    nrow(dplyr::collect(get_ducklake_table("v_large"))), 3
  )

  cleanup_temp_ducklake(lake)
})

test_that("create_view replace semantics work", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:3), "test_view_repl")

  get_ducklake_table("test_view_repl") |>
    dplyr::filter(id > 1) |>
    create_view("v_repl")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("v_repl"))), 2)

  # replace = TRUE swaps the definition
  get_ducklake_table("test_view_repl") |>
    dplyr::filter(id > 2) |>
    create_view("v_repl")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("v_repl"))), 1)

  # replace = FALSE refuses to overwrite
  expect_error(
    get_ducklake_table("test_view_repl") |>
      create_view("v_repl", replace = FALSE)
  )

  cleanup_temp_ducklake(lake)
})

test_that("drop_view removes the view and only the view", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2), "test_view_drop")
  get_ducklake_table("test_view_drop") |> create_view("v_drop")

  drop_view("v_drop")

  expect_false("v_drop" %in% list_ducklake_tables()$table_name)
  # the base table is untouched
  expect_equal(
    nrow(dplyr::collect(get_ducklake_table("test_view_drop"))), 2
  )

  cleanup_temp_ducklake(lake)
})

test_that("views survive detach and re-attach", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:3), "test_view_persist")
  get_ducklake_table("test_view_persist") |>
    dplyr::filter(id >= 2) |>
    create_view("v_persist")

  detach_ducklake(lake$ducklake_name)
  attach_ducklake(lake$ducklake_name, lake_path = lake$lake_path)

  result <- get_ducklake_table("v_persist") |> dplyr::collect()
  expect_equal(nrow(result), 2)

  cleanup_temp_ducklake(lake)
})

test_that("views compose with further dplyr verbs", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(
    data.frame(grp = c("a", "a", "b"), x = c(1, 2, 10)),
    "test_view_compose"
  )
  get_ducklake_table("test_view_compose") |>
    dplyr::filter(x < 10) |>
    create_view("v_compose")

  result <- get_ducklake_table("v_compose") |>
    dplyr::summarise(total = sum(x, na.rm = TRUE), .by = grp) |>
    dplyr::collect()
  expect_equal(result$total, 3)

  cleanup_temp_ducklake(lake)
})

test_that("create_view rejects non-lazy input", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  expect_error(
    create_view(data.frame(id = 1), "v_bad"),
    "lazy table"
  )

  cleanup_temp_ducklake(lake)
})

test_that("create_view carries a view's column comments across a replace", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, grp = c("a", "b", "a"), v = 1:3), "src_v")
  suppressMessages({
    get_ducklake_table("src_v") |> dplyr::filter(grp == "a") |> create_view("a_rows")
    set_table_comment("a_rows", "rows in group a")
    set_column_comments("a_rows", id = "row id", v = "value")
  })
  before <- get_table_comments("a_rows")
  expect_equal(before$comment[before$object_type == "column"], c("row id", "value"))

  # DuckLake drops the comments with the replaced entry; the replace puts
  # back the ones whose columns the new view still has
  suppressMessages(
    get_ducklake_table("src_v") |>
      dplyr::filter(grp == "b") |>
      dplyr::select(id, grp) |>
      create_view("a_rows")
  )
  after <- get_table_comments("a_rows")
  expect_equal(after$comment[after$object_type == "view"], "rows in group a")
  expect_equal(after$column_name[after$object_type == "column"], "id")
  expect_equal(after$comment[after$object_type == "column"], "row id")
  expect_equal(nrow(list_table_snapshots()) > 0, TRUE)
})
