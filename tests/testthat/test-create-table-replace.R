# Tests for create_table(replace = TRUE), the one call that creates a table
# on a script's first run and rewrites it on every later one, and for the
# existence guard that came to replace_table() with it.

temp_objects <- function(conn) {
  c(
    DBI::dbGetQuery(
      conn, "SELECT table_name AS name FROM duckdb_tables() WHERE temporary AND NOT internal"
    )$name,
    DBI::dbGetQuery(
      conn, "SELECT view_name AS name FROM duckdb_views() WHERE temporary AND NOT internal"
    )$name
  )
}

column_comment <- function(table_name, column) {
  comments <- get_table_comments(table_name)
  comments$comment[which(comments$column_name == column)]
}

test_that("create_table(replace = TRUE) creates a missing table on the plain create path", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:6, grp = rep(c("a", "b"), 3), v = 6:1)
  attr(df$v, "label") <- "Value"

  # Nothing to replace, so the rewrite machinery must stay out of it
  local_mocked_bindings(rewrite_table = function(...) stop("rewrite path taken"))
  suppressMessages(create_table(df, "fresh", replace = TRUE, partition_by = "grp"))

  expect_equal(nrow(dplyr::collect(get_ducklake_table("fresh"))), 6)
  expect_equal(column_comment("fresh", "v"), "Value")
  expect_equal(get_table_partitions("fresh")$column_name, "grp")
  expect_equal(nrow(list_table_snapshots("fresh")), 1)
})

test_that("create_table(replace = TRUE) rewrites an existing table and carries its metadata", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:6, grp = rep(c("a", "b"), 3), v = 6:1)
  suppressMessages({
    create_table(df, "keep")
    set_table_comment("keep", "Kept table")
    set_column_comments("keep", id = "Identifier")
    set_table_partitioning("keep", "grp")
    set_table_sorting("keep", "id DESC")
    set_ducklake_option("parquet_compression", "zstd", table_name = "keep")
  })
  before <- max(list_table_snapshots()$snapshot_id)

  df2 <- cbind(df, w = df$v * 2)
  suppressMessages(create_table(df2, "keep", replace = TRUE))

  got <- dplyr::collect(get_ducklake_table("keep"))
  expect_equal(names(got), c("id", "grp", "v", "w"))
  expect_equal(nrow(got), 6)
  comments <- get_table_comments("keep")
  expect_equal(comments$comment[which(comments$object_type == "table")], "Kept table")
  expect_equal(column_comment("keep", "id"), "Identifier")
  expect_equal(get_table_partitions("keep")$column_name, "grp")
  sorting <- get_table_sorting("keep")
  expect_equal(sorting$expression, "id")
  expect_equal(sorting$sort_direction, "DESC")
  expect_equal(table_options_of(lake, "keep"), c(parquet_compression = "zstd"))
  # One snapshot, and nothing staged left behind
  expect_equal(max(list_table_snapshots()$snapshot_id), before + 1)
  expect_length(temp_objects(lake$conn), 0)
})

test_that("an explicit layout on replace wins for its piece and leaves the rest", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:6, grp = rep(c("a", "b"), 3), v = 6:1)
  suppressMessages(create_table(
    df, "layout", partition_by = "grp", sort_by = "id",
    options = list(parquet_compression = "zstd", data_inlining_row_limit = 0)
  ))

  suppressMessages(create_table(df, "layout", replace = TRUE, partition_by = "id"))
  expect_equal(get_table_partitions("layout")$column_name, "id")
  expect_equal(get_table_sorting("layout")$expression, "id")
  expect_equal(
    table_options_of(lake, "layout"),
    c(data_inlining_row_limit = "0", parquet_compression = "zstd")
  )

  # Options merge one by one: the option named changes, the other stays
  suppressMessages(create_table(
    df, "layout", replace = TRUE, options = list(parquet_compression = "snappy")
  ))
  expect_equal(
    table_options_of(lake, "layout"),
    c(data_inlining_row_limit = "0", parquet_compression = "snappy")
  )
  expect_equal(get_table_partitions("layout")$column_name, "id")

  # An explicit key on a column the new table lacks is not filtered away:
  # the rewrite fails and rolls back
  expect_error(
    suppressMessages(
      create_table(df[c("id", "v")], "layout", replace = TRUE, partition_by = "grp")
    ),
    "grp"
  )
  expect_equal(names(dplyr::collect(get_ducklake_table("layout"))), c("id", "grp", "v"))
  expect_equal(get_table_partitions("layout")$column_name, "id")
  expect_length(temp_objects(lake$conn), 0)
})

test_that("labels governs both outcomes of create_table(replace = TRUE)", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:3, v = c(10, 20, 30))
  attr(df$v, "label") <- "Value"

  suppressMessages(create_table(df, "lab", replace = TRUE, labels = FALSE))
  expect_equal(nrow(get_table_comments("lab")), 0)

  # Existing comments carry over when the source's labels are not stored
  suppressMessages(set_column_comments("lab", id = "Identifier"))
  suppressMessages(create_table(df, "lab", replace = TRUE, labels = FALSE))
  comments <- get_table_comments("lab")
  expect_equal(comments$column_name, "id")
  expect_equal(comments$comment, "Identifier")

  # and a stored label wins over the carried comment for its column
  attr(df$id, "label") <- "Row id"
  suppressMessages(create_table(df, "lab", replace = TRUE))
  comments <- get_table_comments("lab")
  expect_equal(comments$comment[order(comments$column_name)], c("Row id", "Value"))
})

test_that("create_table(replace = TRUE) joins a transaction and rewrites inside DuckDB", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:5, v = c(10, 20, 30, 40, 50))
  suppressMessages(create_table(df, "txn"))
  before <- max(list_table_snapshots()$snapshot_id)

  suppressMessages(with_transaction(
    create_table(df[1:4, ], "txn", replace = TRUE),
    author = "Tester",
    commit_message = "Rebuilt with four rows"
  ))
  snapshots <- list_table_snapshots()
  expect_equal(max(snapshots$snapshot_id), before + 1)
  expect_equal(
    snapshots$commit_message[snapshots$snapshot_id == before + 1],
    "Rebuilt with four rows"
  )
  expect_equal(nrow(DBI::dbGetQuery(lake$conn, "SELECT * FROM txn")), 4)

  # A table created earlier in the same transaction is seen, and replaced
  suppressMessages(with_transaction({
    create_table(df, "txn_new")
    create_table(df[1:2, ], "txn_new", replace = TRUE)
  }))
  expect_equal(nrow(DBI::dbGetQuery(lake$conn, "SELECT * FROM txn_new")), 2)

  # A pipeline that reads the target rewrites it without collecting
  local_mocked_bindings(
    collect = function(x, ...) stop("collected into R"),
    .package = "dplyr"
  )
  suppressMessages(
    get_ducklake_table("txn") |>
      dplyr::filter(v > 10) |>
      dplyr::mutate(w = v * 2) |>
      create_table("txn", replace = TRUE)
  )
  result <- DBI::dbGetQuery(lake$conn, "SELECT * FROM txn ORDER BY id")
  expect_equal(result$id, 2:4)
  expect_equal(result$w, c(40, 60, 80))
})

test_that("replace = FALSE still refuses an existing table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  suppressMessages(create_table(data.frame(id = 1:3), "once"))
  expect_error(create_table(data.frame(id = 1), "once"), "already exists")
  expect_error(create_table(data.frame(id = 1), "once", replace = FALSE), "already exists")
  expect_equal(nrow(DBI::dbGetQuery(lake$conn, "SELECT * FROM once")), 3)
})

test_that("a view of the name is refused by create_table(replace = TRUE) and replace_table()", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:3, v = c(1, 2, 3))
  suppressMessages(create_table(df, "base"))
  suppressMessages(
    get_ducklake_table("base") |> dplyr::filter(v > 1) |> create_view("v_base")
  )

  expect_error(create_table(df, "v_base", replace = TRUE), "is a view")
  expect_error(replace_table(df, "v_base"), "is a view")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("v_base"))), 2)
  listed <- list_ducklake_tables()
  expect_equal(listed$type[listed$table_name == "v_base"], "view")
})

test_that("create_table(replace = TRUE) follows nested and lake-prefixed names", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:4, region = c("n", "s", "n", "s"), amount = 4:1)
  suppressMessages({
    create_schema("sales.europe")
    create_table(df, "orders")
    create_table(df, "sales.europe.orders", partition_by = "region")
    set_column_comments("sales.europe.orders", amount = "Order amount")
  })
  before <- max(list_table_snapshots()$snapshot_id)

  suppressMessages(create_table(df[1:2, ], "sales.europe.orders", replace = TRUE))
  expect_equal(nrow(dplyr::collect(get_ducklake_table("sales.europe.orders"))), 2)
  expect_equal(nrow(dplyr::collect(get_ducklake_table("orders"))), 4)
  expect_equal(get_table_partitions("sales.europe.orders")$column_name, "region")
  expect_equal(column_comment("sales.europe.orders", "amount"), "Order amount")
  expect_equal(max(list_table_snapshots()$snapshot_id), before + 1)

  # A leading lake name is read as the lake, not as a schema
  suppressMessages(create_table(
    df[1:3, ], paste0(lake$ducklake_name, ".sales.europe.orders"), replace = TRUE
  ))
  expect_equal(nrow(dplyr::collect(get_ducklake_table("sales.europe.orders"))), 3)
  expect_equal(max(list_table_snapshots()$snapshot_id), before + 2)

  # A missing schema is an error on the create path, never created
  expect_error(create_table(df, "nosuch.orders", replace = TRUE), "nosuch")
  expect_false(ducklake:::schema_exists("nosuch", lake$ducklake_name, lake$conn))
})

test_that("replace_table() refuses a missing table and points at create_table(replace = TRUE)", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  expect_error(replace_table(data.frame(id = 1), "nope"), "does not exist")
  expect_error(replace_table(data.frame(id = 1), "nope"), "create_table")
  expect_false("nope" %in% list_ducklake_tables()$table_name)
})

test_that("restore_table_version() brings a dropped table back", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  suppressMessages(create_table(data.frame(id = 1:3), "gone"))
  v1 <- max(list_table_snapshots("gone")$snapshot_id)
  DBI::dbExecute(
    lake$conn, sprintf("DROP TABLE %s", ducklake:::table_sql("gone", lake$conn))
  )
  expect_false("gone" %in% list_ducklake_tables()$table_name)

  # The restore rebuilds through DROP IF EXISTS + CREATE, a path the
  # replace_table() guard must leave alone
  suppressMessages(restore_table_version("gone", version = v1))
  expect_equal(dplyr::collect(get_ducklake_table("gone"))$id, 1:3)
})

test_that("a failed rewrite leaves the table as it was and no staged copy", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:5, v = 5:1)
  suppressMessages(create_table(df, "atomic"))

  local_mocked_bindings(
    reapply_table_comments = function(...) stop("simulated failure after drop")
  )
  expect_error(
    create_table(df[1:2, ], "atomic", replace = TRUE),
    "simulated failure after drop"
  )
  expect_equal(nrow(dplyr::collect(get_ducklake_table("atomic"))), 5)
  expect_length(temp_objects(lake$conn), 0)
})

test_that("a file path replaces an existing table through either verb", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  suppressMessages(create_table(data.frame(id = 1:3, v = 3:1), "from_file"))
  csv <- tempfile(fileext = ".csv")
  on.exit(unlink(csv), add = TRUE)

  utils::write.csv(data.frame(id = 1:10, v = 10:1), csv, row.names = FALSE)
  suppressMessages(create_table(csv, "from_file", replace = TRUE))
  expect_equal(nrow(dplyr::collect(get_ducklake_table("from_file"))), 10)

  utils::write.csv(data.frame(id = 1:7, v = 7:1), csv, row.names = FALSE)
  suppressMessages(replace_table(csv, "from_file"))
  expect_equal(nrow(dplyr::collect(get_ducklake_table("from_file"))), 7)
})
