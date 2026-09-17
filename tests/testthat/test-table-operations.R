# UNIT TESTS: Table operations (replace_table)
#
# These tests verify the table-level operations for modifying DuckLake tables.
# Note: update_table() is internal and tested indirectly through ducklake_exec()

test_that("replace_table adds new columns and creates snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create initial table
  initial_data <- data.frame(
    id = 1:5,
    value = c(10, 20, 30, 40, 50),
    stringsAsFactors = FALSE
  )
  with_transaction({
    create_table(initial_data, "test_replace_add_col")
  })
  
  # Get initial snapshot count
  snapshots_before <- list_table_snapshots("test_replace_add_col")
  
  # Use replace_table to add new column
  with_transaction({
    get_ducklake_table("test_replace_add_col") |>
      dplyr::mutate(
        doubled = value * 2,
        category = dplyr::if_else(value > 25, "high", "low")
      ) |>
      replace_table("test_replace_add_col")
  })
  
  # Verify new columns exist
  result <- get_ducklake_table("test_replace_add_col") |> dplyr::collect()
  expect_true("doubled" %in% names(result))
  expect_true("category" %in% names(result))
  expect_equal(result$doubled, c(20, 40, 60, 80, 100))
  
  # Verify new snapshot was created
  snapshots_after <- list_table_snapshots("test_replace_add_col")
  expect_gt(nrow(snapshots_after), nrow(snapshots_before))
  
  cleanup_temp_ducklake(lake)
})

test_that("replace_table removes columns", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create initial table with multiple columns
  initial_data <- data.frame(
    id = 1:3,
    col1 = c("a", "b", "c"),
    col2 = c("x", "y", "z"),
    col3 = c(10, 20, 30),
    stringsAsFactors = FALSE
  )
  with_transaction({
    create_table(initial_data, "test_replace_remove_col")
  })
  
  # Use replace_table to remove columns
  with_transaction({
    get_ducklake_table("test_replace_remove_col") |>
      dplyr::select(id, col1) |>
      replace_table("test_replace_remove_col")
  })
  
  # Verify only selected columns remain
  result <- get_ducklake_table("test_replace_remove_col") |> dplyr::collect()
  expect_equal(names(result), c("id", "col1"))
  expect_false("col2" %in% names(result))
  expect_false("col3" %in% names(result))
  
  cleanup_temp_ducklake(lake)
})

test_that("replace_table handles complex transformations", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create initial table
  initial_data <- data.frame(
    id = 1:10,
    category = rep(c("A", "B"), 5),
    value = 1:10,
    stringsAsFactors = FALSE
  )
  with_transaction({
    create_table(initial_data, "test_replace_complex")
  })
  
  # Apply complex transformation
  with_transaction({
    get_ducklake_table("test_replace_complex") |>
      dplyr::filter(category == "A") |>
      dplyr::mutate(
        value_squared = value * value,
        value_log = log(value)
      ) |>
      dplyr::arrange(dplyr::desc(value)) |>
      replace_table("test_replace_complex")
  })
  
  # Verify transformation
  result <- get_ducklake_table("test_replace_complex") |> dplyr::collect()
  
  # Should only have category A rows
  expect_equal(nrow(result), 5)
  expect_true(all(result$category == "A"))
  
  # New columns should exist
  expect_true("value_squared" %in% names(result))
  expect_true("value_log" %in% names(result))
  
  cleanup_temp_ducklake(lake)
})

test_that("replace_table respects .quiet parameter", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create table
  initial_data <- data.frame(id = 1:3, value = c("a", "b", "c"), stringsAsFactors = FALSE)
  with_transaction({
    create_table(initial_data, "test_replace_quiet")
  })
  
  # Test with .quiet = FALSE
  expect_message(
    get_ducklake_table("test_replace_quiet") |>
      dplyr::mutate(new_col = "added") |>
      replace_table("test_replace_quiet", .quiet = FALSE),
    "Replacing table"
  )

  # Test with .quiet = TRUE
  expect_no_message(
    get_ducklake_table("test_replace_quiet") |>
      dplyr::select(id, value) |>
      replace_table("test_replace_quiet", .quiet = TRUE)
  )

  cleanup_temp_ducklake(lake)
})

test_that("table operations work in transaction context", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create initial table
  initial_data <- data.frame(id = 1:3, value = c(10, 20, 30), stringsAsFactors = FALSE)
  with_transaction({
    create_table(initial_data, "test_txn_context")
  })
  
  # Use replace_table to add column (creates snapshot)
  with_transaction({
    get_ducklake_table("test_txn_context") |>
      dplyr::mutate(doubled = value * 2) |>
      replace_table("test_txn_context", .quiet = TRUE)
  })
  
  # Verify final state
  result <- get_ducklake_table("test_txn_context") |> dplyr::collect()
  expect_true("doubled" %in% names(result))
  
  cleanup_temp_ducklake(lake)
})

test_that("replace_table keeps the original table when the create fails", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  create_table(mtcars[1:5, ], "replace_atomic")

  # Fail after the DROP and CREATE have run: the transaction must roll back
  local_mocked_bindings(
    reapply_table_comments = function(...) stop("simulated failure after drop")
  )
  expect_error(
    get_ducklake_table("replace_atomic") |>
      dplyr::filter(mpg > 0) |>
      replace_table("replace_atomic"),
    "simulated failure after drop"
  )

  result <- get_ducklake_table("replace_atomic") |> dplyr::collect()
  expect_equal(nrow(result), 5)

  cleanup_temp_ducklake(lake)
})

test_that("create_table unregisters its temp view when the CREATE fails", {
  skip_if_not_installed("duckdb")

  lake <- create_temp_ducklake()

  local_mocked_bindings(
    db_execute = function(sql, ...) stop("simulated create failure")
  )
  expect_error(
    create_table(mtcars, "view_leak_check"),
    "simulated create failure"
  )

  views <- DBI::dbGetQuery(
    lake$conn,
    "SELECT view_name FROM duckdb_views() WHERE view_name = '__temp_view_view_leak_check'"
  )
  expect_equal(nrow(views), 0)

  cleanup_temp_ducklake(lake)
})

test_that("replace_table carries comments, partition keys, sort order, and options over", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(
    id = 1:6, grp = rep(c("a", "b"), 3), v = c(10, 20, 30, 40, 50, 60)
  )
  attr(df$v, "label") <- "Value (units)"
  suppressMessages({
    create_table(df, "keep_meta")
    set_table_comment("keep_meta", "Six rows")
    set_table_partitioning("keep_meta", "grp")
    set_table_sorting("keep_meta", "id DESC")
    set_ducklake_option("parquet_compression", "zstd", table_name = "keep_meta")

    get_ducklake_table("keep_meta") |>
      dplyr::mutate(w = v * 2) |>
      replace_table("keep_meta")
  })

  comments <- get_table_comments("keep_meta")
  expect_equal(comments$comment[comments$object_type == "table"], "Six rows")
  expect_equal(comments$comment[comments$column_name %in% "v"], "Value (units)")
  expect_equal(get_table_partitions("keep_meta")$column_name, "grp")
  sorting <- get_table_sorting("keep_meta")
  expect_equal(sorting$expression, "id")
  expect_equal(sorting$sort_direction, "DESC")
  opts <- get_ducklake_options(lake$ducklake_name)
  expect_true(any(
    opts$option_name == "parquet_compression" & opts$scope == "TABLE" &
      opts$scope_entry %in% "main.keep_meta" & opts$value == "zstd"
  ))

  # A rewrite that drops the partition column drops that key, keeps the rest
  suppressMessages(
    get_ducklake_table("keep_meta") |>
      dplyr::select(-grp) |>
      replace_table("keep_meta")
  )
  expect_equal(nrow(get_table_partitions("keep_meta")), 0)
  expect_equal(get_table_sorting("keep_meta")$expression, "id")
  expect_equal(
    get_table_comments("keep_meta")$comment[
      get_table_comments("keep_meta")$object_type == "table"
    ],
    "Six rows"
  )
})

test_that("replace_table keeps comments set earlier in the same transaction", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, amount = c(1, 2, 3)), "test_replace_txn_meta")
  suppressMessages(set_table_comment("test_replace_txn_meta", "Committed comment"))

  suppressMessages(with_transaction({
    set_table_comment("test_replace_txn_meta", "Set in this transaction")
    set_column_comments("test_replace_txn_meta", amount = "Amount, doubled below")
    get_ducklake_table("test_replace_txn_meta") |>
      dplyr::mutate(amount = amount * 2) |>
      replace_table("test_replace_txn_meta")
  }))

  comments <- get_table_comments("test_replace_txn_meta")
  expect_equal(
    comments$comment[comments$object_type == "table"],
    "Set in this transaction"
  )
  expect_equal(
    comments$comment[comments$object_type == "column"],
    "Amount, doubled below"
  )
})

test_that("replace_table keeps partition and sort keys set earlier in the same transaction", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(
    data.frame(grp = c("a", "b", "a"), id = 1:3, day = as.Date("2026-01-01") + 0:2),
    "test_replace_txn_keys"
  )

  suppressMessages(with_transaction({
    set_table_partitioning("test_replace_txn_keys", c("grp", "year(day)"))
    set_table_sorting("test_replace_txn_keys", "id DESC NULLS FIRST")
    get_ducklake_table("test_replace_txn_keys") |>
      dplyr::mutate(id = id * 10L) |>
      replace_table("test_replace_txn_keys")
  }))

  partitions <- get_table_partitions("test_replace_txn_keys")
  expect_equal(partitions$column_name, c("grp", "day"))
  expect_equal(partitions$transform, c("identity", "year"))
  sorting <- get_table_sorting("test_replace_txn_keys")
  expect_equal(sorting$expression, "id")
  expect_equal(sorting$sort_direction, "DESC")
  expect_equal(sorting$null_order, "NULLS_FIRST")
})

test_that("replace_table leaves keys off that were reset earlier in the same transaction", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(grp = c("a", "b"), id = 1:2), "test_replace_txn_reset")
  suppressMessages({
    set_table_partitioning("test_replace_txn_reset", "grp")
    set_table_sorting("test_replace_txn_reset", "id")
  })

  suppressMessages(with_transaction({
    reset_table_partitioning("test_replace_txn_reset")
    reset_table_sorting("test_replace_txn_reset")
    get_ducklake_table("test_replace_txn_reset") |>
      dplyr::mutate(id = id * 10L) |>
      replace_table("test_replace_txn_reset")
  }))

  expect_equal(nrow(get_table_partitions("test_replace_txn_reset")), 0)
  expect_equal(nrow(get_table_sorting("test_replace_txn_reset")), 0)
})

test_that("keys set in a transaction that was rolled back do not leak into a later replace", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(grp = c("a", "b"), id = 1:2), "test_replace_txn_leak")
  suppressMessages({
    begin_transaction()
    set_table_partitioning("test_replace_txn_leak", "grp")
    rollback_transaction()
  })

  suppressMessages(with_transaction(
    get_ducklake_table("test_replace_txn_leak") |>
      dplyr::mutate(id = id * 10L) |>
      replace_table("test_replace_txn_leak")
  ))

  expect_equal(nrow(get_table_partitions("test_replace_txn_leak")), 0)
})

test_that("pending keys are noted only inside a transaction and follow every rewrite in it", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)
  env <- get_ducklake_env()

  create_table(
    data.frame(grp = c("a", "b"), id = 1:2, extra = c(5, 6)),
    "test_replace_txn_edges"
  )

  # autocommit: the keys are committed at once, so nothing is kept
  suppressMessages(set_table_sorting("test_replace_txn_edges", "extra"))
  key <- paste(lake$ducklake_name, "main", "test_replace_txn_edges", sep = ".")
  expect_null(env$pending_keys[[key]])

  suppressMessages(with_transaction({
    set_table_partitioning("test_replace_txn_edges", "grp")
    set_table_sorting("test_replace_txn_edges", c("id", "extra DESC"))
    # the first rewrite drops a sort column; the second has both keys to keep
    get_ducklake_table("test_replace_txn_edges") |>
      dplyr::select(-extra) |>
      replace_table("test_replace_txn_edges")
    get_ducklake_table("test_replace_txn_edges") |>
      dplyr::mutate(id = id * 10L) |>
      replace_table("test_replace_txn_edges")
  }))

  expect_equal(get_table_partitions("test_replace_txn_edges")$column_name, "grp")
  expect_equal(get_table_sorting("test_replace_txn_edges")$expression, "id")
})

test_that("replace_table inside a transaction warns about table options it cannot carry over", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:2), "test_replace_txn_opts")
  suppressMessages({
    set_ducklake_option("parquet_compression", "zstd", table_name = "test_replace_txn_opts")
    set_ducklake_option("parquet_version", 2, table_name = "test_replace_txn_opts")
  })

  # DuckLake refuses set_option() on a table created in the open transaction,
  # so the rewrite commits and names what is left to do
  expect_warning(
    suppressMessages(with_transaction(
      get_ducklake_table("test_replace_txn_opts") |>
        dplyr::mutate(id = id * 10L) |>
        replace_table("test_replace_txn_opts")
    )),
    "could not be carried over"
  )
  expect_equal(
    sort(dplyr::pull(get_ducklake_table("test_replace_txn_opts"), id)), c(10, 20)
  )
})

test_that("replace_table and restore_table_version carry size and version options over", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  # DuckLake reports these three in a form set_option() does not take back:
  # bytes without a unit, and "V2" for the Parquet version
  table_options <- function() {
    opts <- get_ducklake_options(lake$ducklake_name)
    opts <- opts[opts$scope == "TABLE" & opts$scope_entry %in% "main.test_replace_opt_units", ]
    stats::setNames(opts$value, opts$option_name)[order(opts$option_name)]
  }

  create_table(data.frame(id = 1:2), "test_replace_opt_units")
  suppressMessages({
    set_ducklake_option("target_file_size", "64MB", table_name = "test_replace_opt_units")
    set_ducklake_option("parquet_row_group_size_bytes", "8MiB", table_name = "test_replace_opt_units")
    set_ducklake_option("parquet_version", 2, table_name = "test_replace_opt_units")
  })
  before <- table_options()
  v1 <- max(list_table_snapshots("test_replace_opt_units")$snapshot_id)

  suppressMessages(
    get_ducklake_table("test_replace_opt_units") |>
      dplyr::mutate(id = id * 10L) |>
      replace_table("test_replace_opt_units")
  )
  expect_equal(table_options(), before)

  suppressMessages(restore_table_version("test_replace_opt_units", version = v1))
  expect_equal(table_options(), before)
  expect_equal(
    sort(dplyr::pull(get_ducklake_table("test_replace_opt_units"), id)), c(1, 2)
  )
})

test_that("restore_table_version refuses an open transaction and leaves it alone", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:2), "test_restore_in_txn")
  v1 <- max(list_table_snapshots("test_restore_in_txn")$snapshot_id)
  suppressMessages(
    rows_insert(get_ducklake_table("test_restore_in_txn"), data.frame(id = 3L), by = "id")
  )

  suppressMessages(begin_transaction())
  suppressMessages(
    rows_insert(get_ducklake_table("test_restore_in_txn"), data.frame(id = 4L), by = "id")
  )
  expect_error(
    restore_table_version("test_restore_in_txn", version = v1),
    "open transaction"
  )
  # the caller's pending insert is still there to commit
  expect_true(ducklake:::in_transaction(lake$conn))
  suppressMessages(commit_transaction())
  expect_equal(
    sort(dplyr::pull(get_ducklake_table("test_restore_in_txn"), id)), 1:4
  )
})

test_that("restore_table_version keeps comments and partition keys", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:4, grp = c("a", "a", "b", "b"))
  attr(df$id, "label") <- "Identifier"
  suppressMessages({
    create_table(df, "restore_meta")
    set_table_partitioning("restore_meta", "grp")
  })
  first <- min(list_table_snapshots("restore_meta")$snapshot_id)

  suppressMessages(
    rows_delete(get_ducklake_table("restore_meta"), data.frame(id = 1L), by = "id")
  )
  suppressMessages(restore_table_version("restore_meta", version = first))

  expect_equal(nrow(dplyr::collect(get_ducklake_table("restore_meta"))), 4)
  expect_equal(get_table_partitions("restore_meta")$column_name, "grp")
  comments <- get_table_comments("restore_meta")
  expect_equal(comments$comment[comments$column_name %in% "id"], "Identifier")
})

test_that("create_table() from a lazy table runs in DuckDB and carries labels", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(
    id = 1:5, v = c(10, 20, 30, 40, 50), grp = c("a", "b", "a", "b", "a")
  )
  attr(df$v, "label") <- "Value"
  attr(df$grp, "label") <- "Group"
  suppressMessages(create_table(df, "lazy_src"))

  # The query must run as CREATE TABLE ... AS: collecting would be a bug
  local_mocked_bindings(
    collect = function(x, ...) stop("collected into R"),
    .package = "dplyr"
  )
  suppressMessages(
    get_ducklake_table("lazy_src") |>
      dplyr::filter(v > 10) |>
      dplyr::mutate(w = v * 2) |>
      dplyr::select(id, v, w, category = grp) |>
      create_table("lazy_dst")
  )

  result <- DBI::dbGetQuery(
    get_ducklake_connection(), "SELECT * FROM lazy_dst ORDER BY id"
  )
  expect_equal(result$id, 2:5)
  expect_equal(result$w, c(40, 60, 80, 100))

  comments <- get_table_comments("lazy_dst")
  expect_equal(comments$comment[comments$column_name %in% "v"], "Value")
  # renamed and derived columns start without a label
  expect_false("category" %in% comments$column_name)
  expect_false("w" %in% comments$column_name)
})

test_that("create_table() from a join copies comments from every source", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  a <- data.frame(id = 1:3, x = c(1, 2, 3))
  attr(a$x, "label") <- "X value"
  b <- data.frame(id = 1:3, y = c(4, 5, 6))
  attr(b$y, "label") <- "Y value"
  suppressMessages({
    create_table(a, "join_a")
    create_table(b, "join_b")
    get_ducklake_table("join_a") |>
      dplyr::inner_join(get_ducklake_table("join_b"), by = "id") |>
      create_table("join_ab")
  })

  comments <- get_table_comments("join_ab")
  expect_equal(comments$comment[comments$column_name %in% "x"], "X value")
  expect_equal(comments$comment[comments$column_name %in% "y"], "Y value")
  expect_equal(nrow(dplyr::collect(get_ducklake_table("join_ab"))), 3)
})

test_that("lazy tables on another connection still load through R", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  other <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(other, shutdown = TRUE), add = TRUE)
  DBI::dbWriteTable(other, "remote", data.frame(id = 1:4, v = c(1, 2, 3, 4)))

  suppressMessages(
    dplyr::tbl(other, "remote") |>
      dplyr::filter(v > 1) |>
      create_table("from_other")
  )
  expect_equal(nrow(dplyr::collect(get_ducklake_table("from_other"))), 3)

  suppressMessages(dplyr::tbl(other, "remote") |> replace_table("from_other"))
  expect_equal(nrow(dplyr::collect(get_ducklake_table("from_other"))), 4)
})

test_that("replace_table() from a lazy table rewrites inside DuckDB", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  df <- data.frame(id = 1:6, grp = rep(c("a", "b"), 3), v = 1:6 * 10)
  attr(df$v, "label") <- "Value"
  suppressMessages({
    create_table(df, "inplace")
    set_table_partitioning("inplace", "grp")
  })

  local_mocked_bindings(
    collect = function(x, ...) stop("collected into R"),
    .package = "dplyr"
  )
  suppressMessages(
    get_ducklake_table("inplace") |>
      dplyr::filter(id > 2) |>
      dplyr::mutate(w = v * 2) |>
      replace_table("inplace")
  )

  conn <- get_ducklake_connection()
  result <- DBI::dbGetQuery(conn, "SELECT * FROM inplace ORDER BY id")
  expect_equal(result$id, 3:6)
  expect_equal(result$w, c(60, 80, 100, 120))
  comments <- get_table_comments("inplace")
  expect_equal(comments$comment[comments$column_name %in% "v"], "Value")
  expect_equal(get_table_partitions("inplace")$column_name, "grp")

  # The temporary copy of the result is gone
  temp_tables <- DBI::dbGetQuery(
    conn, "SELECT table_name FROM duckdb_tables() WHERE temporary"
  )
  expect_equal(nrow(temp_tables), 0)
})

test_that("replace_table() from a lazy table inside with_transaction() is one snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()
  on.exit(cleanup_temp_ducklake(lake), add = TRUE)

  create_table(data.frame(id = 1:3, v = c(1, 2, 3)), "txn_lazy")
  before <- nrow(list_table_snapshots())

  suppressMessages(with_transaction(
    get_ducklake_table("txn_lazy") |>
      dplyr::mutate(v = v * 10) |>
      replace_table("txn_lazy"),
    author = "Tester",
    commit_message = "times ten"
  ))

  expect_equal(nrow(list_table_snapshots()) - before, 1)
  expect_equal(sort(dplyr::collect(get_ducklake_table("txn_lazy"))$v), c(10, 20, 30))
  snapshots <- list_table_snapshots("txn_lazy")
  expect_equal(snapshots$commit_message[[nrow(snapshots)]], "times ten")
})

