# UNIT TESTS: rows_* operations
#
# These tests verify the row-level operations: rows_insert(), rows_update(),
# rows_delete(), and rows_upsert().

test_that("rows_insert adds new rows to a table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create initial table
  initial_data <- data.frame(
    id = 1:3,
    value = c("a", "b", "c"),
    stringsAsFactors = FALSE
  )
  create_table(initial_data, "test_insert")
  
  # Insert new rows
  new_rows <- data.frame(
    id = 4:5,
    value = c("d", "e"),
    stringsAsFactors = FALSE
  )
  
  # rows_* functions handle transactions internally
  rows_insert(
    get_ducklake_table("test_insert"),
    new_rows,
    by = "id"
  )
  
  # Verify rows were added
  result <- get_ducklake_table("test_insert") |> dplyr::collect()
  expect_equal(nrow(result), 5)
  expect_true(all(c("d", "e") %in% result$value))
  
  cleanup_temp_ducklake(lake)
})

test_that("rows_insert handles conflict parameter", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create initial table
  initial_data <- data.frame(id = 1:3, value = c("a", "b", "c"), stringsAsFactors = FALSE)
  create_table(initial_data, "test_conflict")
  
  # Try to insert conflicting row with conflict = "ignore"
  conflicting_row <- data.frame(id = 2, value = "conflicting", stringsAsFactors = FALSE)
  
  # rows_* functions handle transactions internally
  rows_insert(
    get_ducklake_table("test_conflict"),
    conflicting_row,
    by = "id",
    conflict = "ignore"
  )
  
  # Original row should remain unchanged
  result <- get_ducklake_table("test_conflict") |> 
    dplyr::filter(id == 2) |> 
    dplyr::collect()
  expect_equal(result$value, "b")
  
  cleanup_temp_ducklake(lake)
})

test_that("rows_update modifies existing rows", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create initial table
  initial_data <- data.frame(
    id = 1:3,
    value = c("a", "b", "c"),
    status = c("old", "old", "old"),
    stringsAsFactors = FALSE
  )
  create_table(initial_data, "test_update")
  
  # Update specific rows
  updates <- data.frame(
    id = c(1, 3),
    value = c("updated_a", "updated_c"),
    status = c("new", "new"),
    stringsAsFactors = FALSE
  )
  
  # rows_* functions handle transactions internally
  rows_update(
    get_ducklake_table("test_update"),
    updates,
    by = "id"
  )
  
  # Verify updates
  result <- get_ducklake_table("test_update") |> 
    dplyr::arrange(id) |> 
    dplyr::collect()
  
  expect_equal(result$value[1], "updated_a")
  expect_equal(result$value[2], "b")  # Unchanged
  expect_equal(result$value[3], "updated_c")
  expect_equal(result$status[1], "new")
  expect_equal(result$status[2], "old")  # Unchanged
  expect_equal(result$status[3], "new")
  
  cleanup_temp_ducklake(lake)
})

test_that("rows_update respects unmatched parameter", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create initial table
  initial_data <- data.frame(id = 1:3, value = c("a", "b", "c"), stringsAsFactors = FALSE)
  create_table(initial_data, "test_unmatched")
  
  # Try to update non-existent row with unmatched = "ignore"
  updates <- data.frame(id = 99, value = "nonexistent", stringsAsFactors = FALSE)
  
  # rows_* functions handle transactions internally
  rows_update(
    get_ducklake_table("test_unmatched"),
    updates,
    by = "id",
    unmatched = "ignore"
  )
  
  # Should still have only 3 rows
  result <- get_ducklake_table("test_unmatched") |> dplyr::collect()
  expect_equal(nrow(result), 3)
  
  cleanup_temp_ducklake(lake)
})

test_that("rows_delete removes specified rows", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create initial table
  initial_data <- data.frame(
    id = 1:5,
    value = letters[1:5],
    stringsAsFactors = FALSE
  )
  create_table(initial_data, "test_delete")
  
  # Delete specific rows
  rows_to_delete <- data.frame(id = c(2, 4), stringsAsFactors = FALSE)
  
  # rows_* functions handle transactions internally
  rows_delete(
    get_ducklake_table("test_delete"),
    rows_to_delete,
    by = "id"
  )
  
  # Verify rows were deleted
  result <- get_ducklake_table("test_delete") |> 
    dplyr::arrange(id) |> 
    dplyr::collect()
  
  expect_equal(nrow(result), 3)
  expect_equal(result$id, c(1, 3, 5))
  expect_equal(result$value, c("a", "c", "e"))
  
  cleanup_temp_ducklake(lake)
})

test_that("rows_delete handles empty deletion set", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create initial table
  initial_data <- data.frame(id = 1:3, value = c("a", "b", "c"), stringsAsFactors = FALSE)
  create_table(initial_data, "test_empty_delete")
  
  # Try to delete with empty data frame
  empty_deletes <- data.frame(id = integer(0), stringsAsFactors = FALSE)
  
  # rows_* functions handle transactions internally
  rows_delete(
    get_ducklake_table("test_empty_delete"),
    empty_deletes,
    by = "id",
    unmatched = "ignore"
  )
  
  # Should still have all rows
  result <- get_ducklake_table("test_empty_delete") |> dplyr::collect()
  expect_equal(nrow(result), 3)
  
  cleanup_temp_ducklake(lake)
})

test_that("rows_upsert updates matching rows and inserts new ones", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  initial_data <- data.frame(
    id = 1:3,
    value = c("a", "b", "c"),
    stringsAsFactors = FALSE
  )
  create_table(initial_data, "test_upsert")

  rows_upsert(
    get_ducklake_table("test_upsert"),
    data.frame(id = c(2L, 5L), value = c("updated_b", "e"), stringsAsFactors = FALSE),
    by = "id"
  )

  result <- get_ducklake_table("test_upsert") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$id, c(1L, 2L, 3L, 5L))
  expect_equal(result$value, c("a", "updated_b", "c", "e"))

  cleanup_temp_ducklake(lake)
})

test_that("rows_upsert with only new keys behaves as an insert", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, value = c("a", "b")), "test_upsert_ins")

  rows_upsert(
    get_ducklake_table("test_upsert_ins"),
    data.frame(id = 3:4, value = c("c", "d")),
    by = "id"
  )

  result <- get_ducklake_table("test_upsert_ins") |> dplyr::collect()
  expect_equal(nrow(result), 4)

  cleanup_temp_ducklake(lake)
})

test_that("rows_upsert with only existing keys behaves as an update", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:3, value = c("a", "b", "c")), "test_upsert_upd")

  rows_upsert(
    get_ducklake_table("test_upsert_upd"),
    data.frame(id = c(1L, 3L), value = c("A", "C")),
    by = "id"
  )

  result <- get_ducklake_table("test_upsert_upd") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(nrow(result), 3)
  expect_equal(result$value, c("A", "b", "C"))

  cleanup_temp_ducklake(lake)
})

test_that("rows_upsert with a column subset updates only those columns", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(
    data.frame(id = 1:2, value = c("a", "b"), amount = c(1.5, 2.5)),
    "test_upsert_subset"
  )

  # y omits amount: matched rows keep it, inserted rows get NULL
  rows_upsert(
    get_ducklake_table("test_upsert_subset"),
    data.frame(id = c(2L, 3L), value = c("B", "c")),
    by = "id"
  )

  result <- get_ducklake_table("test_upsert_subset") |>
    dplyr::arrange(id) |>
    dplyr::collect()
  expect_equal(result$value, c("a", "B", "c"))
  expect_equal(result$amount, c(1.5, 2.5, NA))

  cleanup_temp_ducklake(lake)
})

test_that("rows_upsert matches on multiple by columns", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(
    data.frame(
      subj = c("S1", "S1", "S2"),
      visit = c(1L, 2L, 1L),
      value = c("a", "b", "c")
    ),
    "test_upsert_multi"
  )

  rows_upsert(
    get_ducklake_table("test_upsert_multi"),
    data.frame(subj = c("S1", "S2"), visit = c(2L, 2L), value = c("B", "d")),
    by = c("subj", "visit")
  )

  result <- get_ducklake_table("test_upsert_multi") |>
    dplyr::arrange(subj, visit) |>
    dplyr::collect()
  expect_equal(nrow(result), 4)
  expect_equal(result$value, c("a", "B", "c", "d"))

  cleanup_temp_ducklake(lake)
})

test_that("rows_upsert defaults by to the first column of y with a message", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, value = c("a", "b")), "test_upsert_by")

  expect_message(
    rows_upsert(
      get_ducklake_table("test_upsert_by"),
      data.frame(id = 2L, value = "B")
    ),
    'Matching, by = "id"'
  )

  cleanup_temp_ducklake(lake)
})

test_that("rows_upsert works inside with_transaction as a single snapshot", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:3, value = c("a", "b", "c")), "test_upsert_txn")
  before <- max(list_table_snapshots()$snapshot_id)

  with_transaction(
    rows_upsert(
      get_ducklake_table("test_upsert_txn"),
      data.frame(id = c(1L, 9L), value = c("A", "i")),
      by = "id"
    ),
    author = "tester",
    commit_message = "upsert in transaction"
  )

  snapshots <- list_table_snapshots()
  after <- max(snapshots$snapshot_id)
  expect_equal(after - before, 1)
  expect_equal(
    snapshots$commit_message[snapshots$snapshot_id == after],
    "upsert in transaction"
  )

  cleanup_temp_ducklake(lake)
})

test_that("rows_upsert records updates and inserts in the change feed", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, value = c("a", "b")), "test_upsert_feed")

  rows_upsert(
    get_ducklake_table("test_upsert_feed"),
    data.frame(id = c(2L, 3L), value = c("B", "c")),
    by = "id"
  )

  latest <- max(list_table_snapshots()$snapshot_id)
  changes <- get_table_changes("test_upsert_feed", latest, latest) |>
    dplyr::collect()
  expect_setequal(
    changes$change_type,
    c("insert", "update_preimage", "update_postimage")
  )

  cleanup_temp_ducklake(lake)
})

test_that("rows_upsert rejects bad input", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, value = c("a", "b")), "test_upsert_err")

  # y column absent from x
  expect_error(
    rows_upsert(
      get_ducklake_table("test_upsert_err"),
      data.frame(id = 1L, extra = 1),
      by = "id"
    ),
    "must exist in"
  )

  # modified pipeline as target
  expect_error(
    rows_upsert(
      get_ducklake_table("test_upsert_err") |> dplyr::filter(id > 1),
      data.frame(id = 1L, value = "x"),
      by = "id"
    ),
    "unmodified table reference"
  )

  # by column missing from y
  expect_error(
    rows_upsert(
      get_ducklake_table("test_upsert_err"),
      data.frame(value = "x"),
      by = "id"
    ),
    "must exist in both"
  )

  cleanup_temp_ducklake(lake)
})

test_that("rows_upsert with in_place = FALSE leaves the table unchanged", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")

  lake <- create_temp_ducklake()

  create_table(data.frame(id = 1:2, value = c("a", "b")), "test_upsert_lazy")

  preview <- rows_upsert(
    get_ducklake_table("test_upsert_lazy"),
    data.frame(id = c(2L, 3L), value = c("B", "c")),
    by = "id",
    in_place = FALSE
  ) |>
    dplyr::collect()

  expect_equal(nrow(preview), 3)
  stored <- get_ducklake_table("test_upsert_lazy") |> dplyr::collect()
  expect_equal(nrow(stored), 2)
  expect_setequal(stored$value, c("a", "b"))

  cleanup_temp_ducklake(lake)
})

test_that("merge SQL builders quote identifiers and assemble clauses", {
  skip_if_not_installed("duckdb")

  con <- DBI::ANSI()

  on_sql <- merge_on_sql(c("id", "Sepal.Length"), con, "t", "s")
  expect_equal(on_sql, 't."id" = s."id" AND t."Sepal.Length" = s."Sepal.Length"')

  set_sql <- merge_update_set_sql(c("value", "amt"), con, "s")
  expect_equal(set_sql, '"value" = s."value", "amt" = s."amt"')

  sql <- build_merge_sql(
    target_sql = '"tgt"',
    source_sql = "SELECT 1 AS id",
    on_sql = 't."id" = s."id"',
    clauses = c("WHEN MATCHED THEN UPDATE SET \"v\" = s.\"v\"",
                "WHEN NOT MATCHED THEN INSERT BY NAME"),
    target_alias = "t",
    source_alias = "s"
  )
  expect_match(sql, "^MERGE INTO \"tgt\" AS t USING")
  expect_match(sql, "WHEN NOT MATCHED THEN INSERT BY NAME")
})

test_that("rows operations work with in_place = TRUE by default", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create table
  initial_data <- data.frame(id = 1:3, value = c("a", "b", "c"), stringsAsFactors = FALSE)
  create_table(initial_data, "test_in_place")
  
  # Verify in_place defaults work (should not throw errors)
  # rows_* functions handle transactions internally
  rows_update(
    get_ducklake_table("test_in_place"),
    data.frame(id = 1, value = "updated", stringsAsFactors = FALSE),
    by = "id"
    # in_place defaults to TRUE
  )
  
  result <- get_ducklake_table("test_in_place") |> 
    dplyr::filter(id == 1) |> 
    dplyr::collect()
  expect_equal(result$value, "updated")
  
  cleanup_temp_ducklake(lake)
})
