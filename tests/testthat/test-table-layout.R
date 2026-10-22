# Tests for create_table()'s layout arguments and for table options set
# and reset through ALTER TABLE.

test_that("create_table puts keys and options on before the first load", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:30, grp = rep(c("a", "b", "c"), 10), v = 30:1)
  attr(df$v, "label") <- "Value"
  suppressMessages(create_table(
    df, "layout_df",
    partition_by = "grp", sort_by = "id DESC",
    options = list(data_inlining_row_limit = 0, parquet_compression = "zstd")
  ))

  expect_equal(get_table_partitions("layout_df")$column_name, "grp")
  sorting <- get_table_sorting("layout_df")
  expect_equal(sorting$expression, "id")
  expect_equal(sorting$sort_direction, "DESC")
  expect_equal(
    table_options_of(lake, "layout_df"),
    c(data_inlining_row_limit = "0", parquet_compression = "zstd")
  )

  # The first load honored the keys: one file per partition value
  expect_equal(nrow(list_ducklake_files("layout_df")), 3)
  collected <- dplyr::collect(get_ducklake_table("layout_df"))
  expect_equal(nrow(collected), 30)
  expect_equal(attr(collected$v, "label"), "Value")
  # and it all landed as one snapshot
  expect_equal(nrow(list_table_snapshots("layout_df")), 1)
})

test_that("an inlining limit given at creation governs the first rows", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3), "inlined_default")
  expect_equal(nrow(list_ducklake_files("inlined_default")), 0)

  create_table(
    data.frame(id = 1:3), "inlined_off",
    options = list(data_inlining_row_limit = 0)
  )
  expect_equal(nrow(list_ducklake_files("inlined_off")), 1)
})

test_that("create_table from a query takes its layout inside a caller's transaction", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:12, grp = rep(c("x", "y"), 6), amount = 12:1)
  attr(df$amount, "label") <- "Amount"
  suppressMessages(create_table(df, "layout_src"))
  s0 <- max(list_table_snapshots()$snapshot_id)

  suppressMessages(with_transaction(
    get_ducklake_table("layout_src") |>
      dplyr::filter(id > 2) |>
      create_table(
        "layout_derived",
        partition_by = "grp", sort_by = "amount",
        options = list(parquet_compression = "zstd")
      ),
    commit_message = "derive with layout"
  ))

  expect_equal(get_table_partitions("layout_derived")$column_name, "grp")
  expect_equal(get_table_sorting("layout_derived")$expression, "amount")
  expect_equal(
    table_options_of(lake, "layout_derived"),
    c(parquet_compression = "zstd")
  )
  collected <- dplyr::collect(get_ducklake_table("layout_derived"))
  expect_equal(nrow(collected), 10)
  expect_equal(attr(collected$amount, "label"), "Amount")
  snaps <- list_table_snapshots()
  expect_equal(max(snaps$snapshot_id), s0 + 1)
  expect_equal(
    snaps$commit_message[snaps$snapshot_id == s0 + 1], "derive with layout"
  )
})

test_that("create_table from a file takes a layout", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  csv_path <- tempfile(fileext = ".csv")
  on.exit(unlink(csv_path), add = TRUE)
  utils::write.csv(
    data.frame(id = 20:1, grp = rep(c("a", "b"), 10)), csv_path,
    row.names = FALSE
  )

  create_table(csv_path, "layout_csv", sort_by = "id", partition_by = "grp")
  expect_equal(get_table_sorting("layout_csv")$expression, "id")
  expect_equal(get_table_partitions("layout_csv")$column_name, "grp")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("layout_csv"))), 20)
  expect_equal(nrow(list_table_snapshots("layout_csv")), 1)
})

test_that("create_table validates its layout before creating anything", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:2, grp = c("a", "b"))
  expect_error(
    create_table(df, "layout_bad", partition_by = "grp; DROP TABLE x"),
    "Invalid partition expression"
  )
  expect_error(
    create_table(df, "layout_bad", sort_by = "id SIDEWAYS"),
    "Invalid sort expression"
  )
  expect_error(create_table(df, "layout_bad", options = list(1)), "named list")
  expect_error(
    create_table(df, "layout_bad", options = list(`bad name` = 1)),
    "snake_case"
  )
  expect_error(
    create_table(df, "layout_bad", options = list(parquet_compression = c("a", "b"))),
    "single non-missing value"
  )
  expect_false("layout_bad" %in% list_ducklake_tables()$table_name)
})

test_that("set_ducklake_option works on a table created in the open transaction", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  # DuckLake's set_option() refuses a transaction-local table; the ALTER
  # TABLE form behind set_ducklake_option() does not
  suppressMessages(with_transaction({
    create_table(data.frame(id = 1:2), "txn_opt")
    set_ducklake_option("parquet_compression", "zstd", table_name = "txn_opt")
    set_ducklake_option("auto_compact", FALSE, table_name = "txn_opt")
  }))
  expect_equal(
    table_options_of(lake, "txn_opt"),
    c(auto_compact = "false", parquet_compression = "zstd")
  )
})

test_that("reset_ducklake_option removes a table's override", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  suppressMessages(set_ducklake_option("parquet_compression", "gzip"))
  create_table(
    data.frame(id = 1:2), "reset_opt",
    options = list(parquet_compression = "zstd", parquet_version = 2)
  )
  expect_equal(
    table_options_of(lake, "reset_opt"),
    c(parquet_compression = "zstd", parquet_version = "V2")
  )

  expect_message(
    reset_ducklake_option("parquet_compression", table_name = "reset_opt"),
    "reset"
  )
  expect_equal(table_options_of(lake, "reset_opt"), c(parquet_version = "V2"))
  # the lake-wide setting is untouched, and resetting again is a no-op
  opts <- get_ducklake_options(lake$ducklake_name)
  expect_equal(
    opts$value[opts$option_name == "parquet_compression" & opts$scope == "GLOBAL"],
    "gzip"
  )
  suppressMessages(
    expect_no_error(reset_ducklake_option("parquet_compression", table_name = "reset_opt"))
  )

  expect_error(reset_ducklake_option("parquet_compression"), "table_name")
  expect_error(
    reset_ducklake_option("bad option!", table_name = "reset_opt"),
    "snake_case"
  )
})

test_that("render_option_assignments renders and validates a named list", {
  expect_equal(
    ducklake:::render_option_assignments(
      list(auto_compact = FALSE, parquet_version = 2, parquet_compression = "zstd")
    ),
    "auto_compact = false, parquet_version = 2, parquet_compression = 'zstd'"
  )
  expect_error(ducklake:::render_option_assignments(list()), "named list")
  expect_error(ducklake:::render_option_assignments(list("zstd")), "named list")
  expect_error(ducklake:::render_option_assignments("zstd"), "named list")
  expect_error(
    ducklake:::render_option_assignments(list(`Parquet-Version` = 2)),
    "snake_case"
  )
})

test_that("typed_option_value hands reported values back in their own type", {
  expect_identical(ducklake:::typed_option_value("true"), TRUE)
  expect_identical(ducklake:::typed_option_value("false"), FALSE)
  expect_identical(ducklake:::typed_option_value("5"), 5)
  expect_identical(ducklake:::typed_option_value("zstd"), "zstd")
  expect_identical(ducklake:::typed_option_value("64000000 bytes"), "64000000 bytes")
})

test_that("qualified_table spells out lake, schema, and table", {
  expect_equal(ducklake:::qualified_table("lake", NULL, "t"), "lake.main.t")
  expect_equal(ducklake:::qualified_table("lake", "bronze", "t"), "lake.bronze.t")
})
