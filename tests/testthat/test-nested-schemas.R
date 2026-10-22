# Tests for nested schemas (DuckLake 1.1): dotted paths everywhere, listings
# that keep same-named tables apart, comments, rewrites, and the limits.

test_that("split_table_name reads paths and strips an attached lake's name", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  split <- ducklake:::split_table_name

  expect_equal(split("cars"), list(lake = NULL, schema = NULL, table = "cars"))
  expect_equal(
    split("bronze.cars"), list(lake = NULL, schema = "bronze", table = "cars")
  )
  expect_equal(
    split("sales.europe.orders"),
    list(lake = NULL, schema = "sales.europe", table = "orders")
  )
  expect_equal(
    split(paste0(lake$ducklake_name, ".sales.orders")),
    list(lake = lake$ducklake_name, schema = "sales", table = "orders")
  )
  expect_equal(
    split(paste0(lake$ducklake_name, ".orders")),
    list(lake = lake$ducklake_name, schema = NULL, table = "orders")
  )
  expect_error(split("a..b"), "empty")

  quote <- function(x) ducklake:::quote_ident(x, lake$conn)
  expect_equal(
    ducklake:::table_sql("sales.europe.orders", lake$conn),
    quote(paste0(lake$ducklake_name, ".sales.europe.orders"))
  )
  expect_equal(
    ducklake:::table_sql("orders", lake$conn),
    quote(paste0(lake$ducklake_name, ".main.orders"))
  )
  # COMMENT ON takes three qualifiers at most: the lake for a top-level
  # schema, no lake for one level of nesting
  expect_equal(
    ducklake:::comment_target_sql("sales.orders", lake$conn),
    quote(paste0(lake$ducklake_name, ".sales.orders"))
  )
  expect_equal(
    ducklake:::comment_target_sql("sales.europe.orders", lake$conn),
    quote("sales.europe.orders")
  )
  expect_error(
    ducklake:::comment_target_sql("sales.europe.north.orders", lake$conn),
    "two schema levels"
  )
})

test_that("create_schema creates a nested path and its missing parents", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  exists <- function(path) ducklake:::schema_exists(path, lake$ducklake_name, lake$conn)

  expect_message(create_schema("sales.europe"), "parent")
  expect_true(exists("sales"))
  expect_true(exists("sales.europe"))
  # the last component alone is not a schema
  expect_false(exists("europe"))

  expect_message(create_schema("sales.europe"), "already exists")
  expect_error(create_schema("sales.europe", if_not_exists = FALSE), "already exists")
  expect_message(create_schema("sales.europe.north"), "Created schema")
  expect_true(exists("sales.europe.north"))
  expect_error(create_schema("sales..north"), "empty")
})

test_that("same-named tables in main, a schema, and a nested schema stay apart", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  suppressMessages(create_schema("sales.europe"))

  rows <- function(n) data.frame(id = seq_len(n))
  create_table(rows(1), "orders")
  create_table(rows(2), "sales.orders")
  create_table(rows(3), "sales.europe.orders")

  tables <- list_ducklake_tables()
  expect_equal(
    tables$schema_name[tables$table_name == "orders"],
    c("main", "sales", "sales.europe")
  )
  expect_equal(nrow(dplyr::collect(get_ducklake_table("orders"))), 1)
  expect_equal(nrow(dplyr::collect(get_ducklake_table("sales.orders"))), 2)
  expect_equal(nrow(dplyr::collect(get_ducklake_table("sales.europe.orders"))), 3)
  # a leading lake name is stripped
  expect_equal(
    nrow(dplyr::collect(
      get_ducklake_table(paste0(lake$ducklake_name, ".sales.europe.orders"))
    )),
    3
  )

  suppressMessages({
    set_table_comment("sales.europe.orders", "nested")
    set_column_comments("sales.europe.orders", id = "nested id")
    set_table_partitioning("sales.europe.orders", "id")
    set_table_sorting("sales.orders", "id DESC")
  })

  comments <- get_table_comments("sales.europe.orders")
  expect_equal(unique(comments$schema_name), "sales.europe")
  expect_equal(comments$comment[comments$object_type == "table"], "nested")
  expect_equal(nrow(get_table_comments("sales.orders")), 0)
  expect_equal(nrow(get_table_comments("main.orders")), 0)
  # an unqualified name asks about that table name in every schema
  expect_equal(nrow(get_table_comments("orders")), 2)
  expect_equal(get_table_comments()$schema_name, c("sales.europe", "sales.europe"))

  expect_equal(get_table_partitions("sales.europe.orders")$schema_name, "sales.europe")
  expect_equal(nrow(get_table_partitions("main.orders")), 0)
  expect_equal(get_table_partitions()$schema_name, "sales.europe")
  expect_equal(get_table_sorting("sales.orders")$schema_name, "sales")
  expect_equal(nrow(get_table_sorting("sales.europe.orders")), 0)

  info <- get_table_info("sales.europe.orders")
  expect_equal(nrow(info), 1)
  expect_equal(info$schema_name, "sales.europe")
  expect_equal(nrow(get_table_info("main.orders")), 1)
  expect_equal(nrow(get_table_info("orders")), 3)

  # a rewrite of main.orders takes main.orders' metadata, not its
  # namesakes'
  suppressMessages(
    get_ducklake_table("orders") |>
      dplyr::mutate(id = id + 10L) |>
      replace_table("orders")
  )
  expect_equal(nrow(get_table_comments("main.orders")), 0)
  expect_equal(nrow(get_table_partitions("main.orders")), 0)
  expect_equal(comments$comment, get_table_comments("sales.europe.orders")$comment)

  # the snapshot history is keyed by path too (main.orders: create, rewrite)
  expect_equal(nrow(list_table_snapshots("orders")), 2)
  expect_equal(nrow(list_table_snapshots("sales.orders")), 2)
  expect_equal(nrow(list_table_snapshots("sales.europe.orders")), 4)

  collected <- dplyr::collect(get_ducklake_table("sales.europe.orders"))
  expect_equal(attr(collected$id, "label"), "nested id")
})

test_that("rewrites, options, and the change feed follow a nested table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  suppressMessages(create_schema("sales.europe"))

  df <- data.frame(id = 1:6, grp = rep(c("a", "b"), 3), v = 6:1)
  attr(df$v, "label") <- "Value"
  suppressMessages(create_table(
    df, "sales.europe.orders",
    partition_by = "grp", options = list(parquet_compression = "zstd")
  ))
  suppressMessages(set_table_comment("sales.europe.orders", "nested"))
  v1 <- max(list_table_snapshots("sales.europe.orders")$snapshot_id)

  suppressMessages(
    get_ducklake_table("sales.europe.orders") |>
      dplyr::filter(id > 2) |>
      dplyr::mutate(w = v * 2) |>
      replace_table("sales.europe.orders")
  )
  comments <- get_table_comments("sales.europe.orders")
  expect_equal(comments$comment[comments$object_type == "table"], "nested")
  expect_equal(comments$comment[comments$column_name %in% "v"], "Value")
  expect_equal(get_table_partitions("sales.europe.orders")$column_name, "grp")
  # the rewritten table carries the option under its path (the dropped
  # table's row lingers with no entry until its snapshot expires)
  opts <- get_ducklake_options(lake$ducklake_name)
  opts <- opts[opts$scope == "TABLE" & opts$scope_entry %in% "sales.europe.orders", ]
  expect_equal(
    stats::setNames(opts$value, opts$option_name), c(parquet_compression = "zstd")
  )

  expect_true(suppressMessages(
    restore_table_version("sales.europe.orders", version = v1)
  ))
  expect_equal(nrow(dplyr::collect(get_ducklake_table("sales.europe.orders"))), 6)

  changes <- dplyr::collect(get_table_changes("sales.europe.orders"))
  expect_true(nrow(changes) > 0)

  suppressMessages({
    set_ducklake_option("auto_compact", FALSE, table_name = "sales.europe.orders")
    set_inlining_row_limit(0, table_name = "sales.europe.orders")
  })
  expect_equal(get_inlining_row_limit(table_name = "sales.europe.orders"), 0L)
  suppressMessages(
    reset_ducklake_option("auto_compact", table_name = "sales.europe.orders")
  )
  opts <- get_ducklake_options(lake$ducklake_name)
  expect_false(any(opts$option_name == "auto_compact"))
})

test_that("comments on a table nested two levels deep are refused clearly", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  suppressMessages(create_schema("sales.europe.north"))

  df <- data.frame(id = 1:2, v = 1:2)
  attr(df$v, "label") <- "Value"
  # DuckDB's COMMENT ON takes three qualifiers, so the labels are dropped
  # rather than the load failing
  expect_warning(create_table(df, "sales.europe.north.deep"), "could not be stored")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("sales.europe.north.deep"))), 2)
  expect_error(set_table_comment("sales.europe.north.deep", "x"), "two schema levels")
  expect_error(set_column_comments("sales.europe.north.deep", v = "x"), "two schema levels")

  # one level deep is within reach
  suppressMessages(create_table(df, "sales.europe.ok"))
  expect_equal(get_table_comments("sales.europe.ok")$comment, "Value")
})

test_that("views and checks live in nested schemas", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  suppressMessages(create_schema("sales.europe"))
  create_table(data.frame(id = 1:3, grp = c("a", "b", "a")), "sales.europe.orders")

  suppressMessages(
    get_ducklake_table("sales.europe.orders") |>
      dplyr::filter(grp == "a") |>
      create_view("sales.europe.a_orders")
  )
  tables <- list_ducklake_tables()
  expect_equal(tables$schema_name[tables$type == "view"], "sales.europe")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("sales.europe.a_orders"))), 2)

  suppressMessages(set_table_comment("sales.europe.a_orders", "view label"))
  expect_equal(get_table_comments("sales.europe.a_orders")$comment, "view label")
  # a replace keeps the comment
  suppressMessages(
    get_ducklake_table("sales.europe.orders") |>
      dplyr::filter(grp == "b") |>
      create_view("sales.europe.a_orders")
  )
  expect_equal(get_table_comments("sales.europe.a_orders")$comment, "view label")
  suppressMessages(drop_view("sales.europe.a_orders"))
  expect_false("a_orders" %in% list_ducklake_tables()$table_name)

  suppressMessages(
    get_ducklake_table("sales.europe.orders") |>
      create_check("id_positive", id > 0, label = "ids are positive",
                   schema_name = "sales.checks")
  )
  results <- run_checks("sales.checks")
  expect_equal(results$check, "id_positive")
  expect_equal(results$n_fail, 0)
})

test_that("drop_schema drops a nested schema, with cascade for its contents", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  exists <- function(path) ducklake:::schema_exists(path, lake$ducklake_name, lake$conn)
  suppressMessages(create_schema("sales.europe"))
  create_table(data.frame(id = 1:2), "sales.europe.orders")

  expect_error(drop_schema("sales"), "depend")
  expect_message(drop_schema("sales", cascade = TRUE), "Dropped schema")
  expect_false(exists("sales.europe"))
  expect_false(exists("sales"))
  expect_false("orders" %in% list_ducklake_tables()$table_name)
})

test_that("a 1.0 catalog refuses nested schemas and still lists its tables", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake(ducklake_version = "1.0")
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  expect_false(ducklake:::lake_has_nested_schemas(lake$ducklake_name, lake$conn))

  expect_error(create_schema("sales.europe"), "format 1.0")

  # the flat metadata queries still serve a 1.0 catalog
  suppressMessages({
    create_schema("sales")
    create_table(data.frame(id = 1:2), "sales.t")
    set_table_partitioning("sales.t", "id")
  })
  expect_true("sales" %in% list_ducklake_tables()$schema_name)
  expect_equal(get_table_partitions("sales.t")$schema_name, "sales")
  expect_equal(nrow(list_table_snapshots("sales.t")), 2)
  expect_equal(get_table_info("sales.t")$schema_name, "sales")
})
