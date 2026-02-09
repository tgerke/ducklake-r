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
  output_verbose <- capture.output({
    with_transaction({
      get_ducklake_table("test_replace_quiet") |>
        dplyr::mutate(new_col = "added") |>
        replace_table("test_replace_quiet", .quiet = FALSE)
    })
  })
  
  # Should have messages
  expect_true(length(output_verbose) > 0)
  
  # Reset table
  with_transaction({
    get_ducklake_table("test_replace_quiet") |>
      dplyr::select(id, value) |>
      replace_table("test_replace_quiet", .quiet = TRUE)
  })
  
  # Test with .quiet = TRUE
  output_quiet <- capture.output({
    with_transaction({
      get_ducklake_table("test_replace_quiet") |>
        dplyr::mutate(new_col = "added") |>
        replace_table("test_replace_quiet", .quiet = TRUE)
    })
  })
  
  # Should have less output
  expect_true(length(output_quiet) < length(output_verbose))
  
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
