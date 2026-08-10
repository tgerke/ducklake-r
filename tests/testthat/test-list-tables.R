# UNIT TESTS: list_ducklake_tables()

test_that("list_ducklake_tables lists tables and views with types", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1), "test_list_a")
  create_table(data.frame(id = 1), "test_list_b")
  get_ducklake_table("test_list_a") |> create_view("v_list_a")

  listing <- list_ducklake_tables()
  expect_named(listing, c("schema_name", "table_name", "type"))
  expect_setequal(
    listing$table_name, c("test_list_a", "test_list_b", "v_list_a")
  )
  expect_equal(
    listing$type[listing$table_name == "v_list_a"], "view"
  )
  expect_true(all(listing$schema_name == "main"))

  cleanup_temp_ducklake(lake)
})

test_that("list_ducklake_tables returns a typed zero-row frame for an empty lake", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  listing <- list_ducklake_tables()
  expect_equal(nrow(listing), 0)
  expect_named(listing, c("schema_name", "table_name", "type"))

  cleanup_temp_ducklake(lake)
})

test_that("list_ducklake_tables scopes to one lake among several", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake_one <- create_temp_ducklake()
  create_table(data.frame(id = 1), "test_scope_one")

  lake_two <- create_temp_ducklake()
  create_table(data.frame(id = 1), "test_scope_two")

  # the second lake is current: default lists it, the name reaches back
  expect_equal(list_ducklake_tables()$table_name, "test_scope_two")
  expect_equal(
    list_ducklake_tables(lake_one$ducklake_name)$table_name,
    "test_scope_one"
  )

  cleanup_temp_ducklake(lake_two)
  cleanup_temp_ducklake(lake_one)
})
