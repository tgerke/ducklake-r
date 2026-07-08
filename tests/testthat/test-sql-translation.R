# UNIT TESTS: structured dplyr-to-DuckLake SQL translation
#
# update_table() classifies pipelines from dbplyr's structured query objects
# (sql_build()), not rendered SQL text. These tests pin down the
# classification for each supported shape and the refusals for unsupported
# ones, exercised through ducklake_exec() and show_ducklake_query().

test_that("filter + mutate updates only the matching rows", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:4, v = c(10, 20, 30, 40)), "targeted")

  # Non-idempotent operation: any double execution or wrong WHERE shows up
  get_ducklake_table("targeted") |>
    dplyr::filter(id >= 3) |>
    dplyr::mutate(v = v + 1) |>
    ducklake_exec("targeted")

  result <- get_ducklake_table("targeted") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$v, c(10, 20, 31, 41))
})

test_that("string literals containing SQL keywords do not confuse translation", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(
    data.frame(
      id = 1:3,
      note = c("WHERE is it", "x AS y", "SELECT everything")
    ),
    "keywords"
  )

  # Filter values contain WHERE and AS; the old text-based classifier
  # miscounted these as extra SQL clauses
  get_ducklake_table("keywords") |>
    dplyr::filter(note != "WHERE is it") |>
    ducklake_exec("keywords")

  remaining <- dplyr::collect(get_ducklake_table("keywords"))
  expect_setequal(remaining$id, c(2L, 3L))
})

test_that("mutate that adds a new column is refused with a clear message", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, v = c(1, 2, 3)), "nonewcols")

  expect_error(
    get_ducklake_table("nonewcols") |>
      dplyr::mutate(doubled = v * 2) |>
      ducklake_exec("nonewcols"),
    "cannot add columns"
  )

  expect_equal(
    colnames(get_ducklake_table("nonewcols")),
    c("id", "v")
  )
})

test_that("filtered reads from another table append into the target", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:2, v = c(1, 2)), "ins_target")
  create_table(data.frame(id = 3:6, v = c(3, 4, 5, 6)), "ins_staging")

  # The old text-based classifier turned this shape (WHERE, no mutate) into
  # a DELETE on the target table
  get_ducklake_table("ins_staging") |>
    dplyr::filter(v <= 4) |>
    ducklake_exec("ins_target")

  result <- get_ducklake_table("ins_target") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$id, 1:4)
  expect_equal(result$v, c(1, 2, 3, 4))

  # Source table untouched
  expect_equal(nrow(dplyr::collect(get_ducklake_table("ins_staging"))), 4)
})

test_that("inserts match columns by name, not position", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1L, v = 10), "byname_target")
  # Same columns, opposite order
  create_table(data.frame(v = 20, id = 2L), "byname_staging")

  get_ducklake_table("byname_staging") |>
    ducklake_exec("byname_target")

  result <- get_ducklake_table("byname_target") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$id, c(1L, 2L))
  expect_equal(result$v, c(10, 20))
})

test_that("joined sources append into the target", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = integer(0), v = numeric(0)), "join_target")
  create_table(data.frame(id = 1:3), "join_a")
  create_table(data.frame(id = 2:3, v = c(20, 30)), "join_b")

  get_ducklake_table("join_a") |>
    dplyr::inner_join(get_ducklake_table("join_b"), by = "id") |>
    ducklake_exec("join_target")

  result <- get_ducklake_table("join_target") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$id, 2:3)
  expect_equal(result$v, c(20, 30))
})

test_that("pipelines that nest the target in a subquery are refused", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:4, v = c(1, 2, 3, 4)), "nested")

  # Filtering on the mutated column compiles to a subquery over the target
  expect_error(
    get_ducklake_table("nested") |>
      dplyr::mutate(v = v * 2) |>
      dplyr::filter(v > 4) |>
      ducklake_exec("nested"),
    "too complex|subquery"
  )

  # A join involving the target cannot become an in-place statement
  expect_error(
    get_ducklake_table("nested") |>
      dplyr::inner_join(get_ducklake_table("nested"), by = "id") |>
      ducklake_exec("nested"),
    "too complex|subquery"
  )

  # Table untouched after both refusals
  result <- get_ducklake_table("nested") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$v, c(1, 2, 3, 4))

  # But when the filter only touches unmutated columns, dbplyr collapses
  # the pipeline into a single query and it translates cleanly
  get_ducklake_table("nested") |>
    dplyr::mutate(v = v * 2) |>
    dplyr::filter(id > 2) |>
    ducklake_exec("nested")

  result <- get_ducklake_table("nested") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$v, c(1, 2, 6, 8))
})

test_that("arrange, head, and distinct on the target are refused", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:4, v = c(1, 1, 2, 2)), "clauses")

  expect_error(
    get_ducklake_table("clauses") |>
      dplyr::filter(v == 1) |>
      dplyr::mutate(v = v + 1) |>
      utils::head(1) |>
      ducklake_exec("clauses"),
    "too complex"
  )

  expect_error(
    get_ducklake_table("clauses") |>
      dplyr::distinct() |>
      dplyr::mutate(v = v + 1) |>
      ducklake_exec("clauses"),
    "too complex|subquery"
  )

  result <- get_ducklake_table("clauses") |> dplyr::collect()
  expect_equal(sort(result$v), c(1, 1, 2, 2))
})

test_that("quoted identifiers survive translation", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:3, x = c(1, 2, 3))
  names(df)[2] <- "my col"
  create_table(df, "spacey")

  get_ducklake_table("spacey") |>
    dplyr::filter(id > 1) |>
    dplyr::mutate(`my col` = `my col` * 10) |>
    ducklake_exec("spacey")

  result <- get_ducklake_table("spacey") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result[["my col"]], c(1, 20, 30))
})

test_that("unioned sources append into the target; unions reading it are refused", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1L), "union_target")
  create_table(data.frame(id = 2L), "union_a")
  create_table(data.frame(id = 3L), "union_b")

  dplyr::union_all(
    get_ducklake_table("union_a"),
    get_ducklake_table("union_b")
  ) |>
    ducklake_exec("union_target")

  result <- get_ducklake_table("union_target") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$id, 1:3)

  # A set operation that reads the target cannot be an in-place statement
  expect_error(
    dplyr::union_all(
      get_ducklake_table("union_a"),
      get_ducklake_table("union_target")
    ) |>
      ducklake_exec("union_target"),
    "too complex|subquery"
  )
  expect_equal(nrow(dplyr::collect(get_ducklake_table("union_target"))), 3)
})

test_that("split_select_alias finds only the trailing top-level alias", {
  split_select_alias <- ducklake:::split_select_alias

  # No alias: bare columns and stars pass through
  expect_equal(split_select_alias("mpg"), list(expr = "mpg", name = ""))
  expect_equal(split_select_alias("*"), list(expr = "*", name = ""))
  expect_equal(split_select_alias("cars.*"), list(expr = "cars.*", name = ""))

  # Plain alias
  expect_equal(
    split_select_alias("mpg + 1.0 AS mpg"),
    list(expr = "mpg + 1.0", name = "mpg")
  )

  # AS inside parentheses (CAST) is not the alias
  expect_equal(
    split_select_alias("CAST(ROUND(mpg, 0) AS INTEGER) AS mpg"),
    list(expr = "CAST(ROUND(mpg, 0) AS INTEGER)", name = "mpg")
  )
  expect_equal(
    split_select_alias("CAST(mpg AS INTEGER)"),
    list(expr = "CAST(mpg AS INTEGER)", name = "")
  )

  # AS inside a string literal is not the alias
  expect_equal(
    split_select_alias("'x AS y' AS note"),
    list(expr = "'x AS y'", name = "note")
  )

  # Quoted aliases are unquoted, including embedded doubled quotes
  expect_equal(
    split_select_alias('"my col" + 1 AS "my col"'),
    list(expr = '"my col" + 1', name = "my col")
  )
  expect_equal(
    split_select_alias('x AS "a""b"'),
    list(expr = "x", name = 'a"b')
  )
})

test_that("bare_table_name normalizes qualified and quoted references", {
  bare_table_name <- ducklake:::bare_table_name

  expect_equal(bare_table_name("cars"), "cars")
  expect_equal(bare_table_name("CARS"), "cars")
  expect_equal(bare_table_name('"cars"'), "cars")
  expect_equal(bare_table_name("my_lake.main.cars"), "cars")
  expect_equal(bare_table_name('"my_lake"."main"."cars"'), "cars")
})
