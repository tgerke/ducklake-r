# UNIT TESTS: rows_* operations
#
# These tests verify the row-level operations: rows_insert(), rows_update(),
# and rows_delete().

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
