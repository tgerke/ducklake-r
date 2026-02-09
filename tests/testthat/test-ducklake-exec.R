# UNIT TESTS: ducklake_exec() and show_ducklake_query()
#
# These tests verify the execution and query preview functions for DuckLake operations.
# NOTE: ducklake_exec is a low-level function that generates SQL from dplyr queries.
# For most use cases, prefer update_table() or replace_table().

test_that("ducklake_exec infers table_name from get_ducklake_table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create table
  initial_data <- data.frame(id = 1:3, score = c(10, 20, 30), stringsAsFactors = FALSE)
  create_table(initial_data, "test_inferred_name")
  
  # Use ducklake_exec without explicit table_name
  with_transaction({
    result <- get_ducklake_table("test_inferred_name") |>
      dplyr::mutate(score = score * 2) |>
      ducklake_exec()  # table_name inferred from get_ducklake_table()
    
    # Should execute without error
    expect_true(!is.null(result))
  })
  
  cleanup_temp_ducklake(lake)
})

test_that("ducklake_exec fails when table_name cannot be determined", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create table
  initial_data <- data.frame(id = 1:3, value = c("a", "b", "c"), stringsAsFactors = FALSE)
  create_table(initial_data, "test_no_name")
  
  # Try to use ducklake_exec without table_name and without using get_ducklake_table
  conn <- get_ducklake_connection()
  
  expect_error(
    dplyr::tbl(conn, "test_no_name") |>
      dplyr::mutate(value = "updated") |>
      ducklake_exec(),  # No table_name provided and not from get_ducklake_table()
    "table_name must be provided"
  )
  
  cleanup_temp_ducklake(lake)
})

test_that("ducklake_exec respects .quiet parameter", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create table
  initial_data <- data.frame(id = 1:3, value = c(1, 2, 3), stringsAsFactors = FALSE)
  create_table(initial_data, "test_quiet")
  
  # Capture output with .quiet = FALSE
  output_verbose <- capture.output({
    with_transaction({
      get_ducklake_table("test_quiet") |>
        dplyr::mutate(value = value * 2) |>
        ducklake_exec(.quiet = FALSE)
    })
  })
  
  # Should have debug output
  expect_true(any(grepl("SQL", paste(output_verbose, collapse = " "))))
  
  # Capture output with .quiet = TRUE
  output_quiet <- capture.output({
    with_transaction({
      get_ducklake_table("test_quiet") |>
        dplyr::mutate(value = value * 2) |>
        ducklake_exec(.quiet = TRUE)
    })
  })
  
  # Should have minimal or no output
  expect_true(length(output_quiet) < length(output_verbose))
  
  cleanup_temp_ducklake(lake)
})

test_that("show_ducklake_query displays SQL preview", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create table
  initial_data <- data.frame(id = 1:3, value = c("a", "b", "c"), stringsAsFactors = FALSE)
  create_table(initial_data, "test_show_query")
  
  # Capture output from show_ducklake_query
  output <- capture.output({
    result <- get_ducklake_table("test_show_query") |>
      dplyr::mutate(value = "would_update") |>
      show_ducklake_query()
  })
  
  # Should show SQL preview
  expect_true(any(grepl("DuckLake SQL", paste(output, collapse = " "))))
  
  cleanup_temp_ducklake(lake)
})

test_that("show_ducklake_query returns input invisibly", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create table
  initial_data <- data.frame(id = 1:3, value = c(10, 20, 30), stringsAsFactors = FALSE)
  create_table(initial_data, "test_show_invisible")
  
  # Capture the return value
  query <- get_ducklake_table("test_show_invisible") |>
    dplyr::mutate(value = value * 2)
  
  # show_ducklake_query should return the query invisibly
  result <- suppressMessages(show_ducklake_query(query))
  
  expect_s3_class(result, "tbl_lazy")
  
  cleanup_temp_ducklake(lake)
})

test_that("show_ducklake_query works with explicit table_name", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create table
  initial_data <- data.frame(id = 1:3, value = c("a", "b", "c"), stringsAsFactors = FALSE)
  create_table(initial_data, "test_show_explicit")
  
  # Use show_ducklake_query with explicit table_name
  conn <- get_ducklake_connection()
  
  output <- capture.output({
    dplyr::tbl(conn, "test_show_explicit") |>
      dplyr::mutate(value = "preview") |>
      show_ducklake_query(table_name = "test_show_explicit")
  })
  
  # Should show SQL preview
  expect_true(any(grepl("DuckLake SQL", paste(output, collapse = " "))))
  
  cleanup_temp_ducklake(lake)
})

test_that("show_ducklake_query infers table_name from get_ducklake_table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create table
  initial_data <- data.frame(id = 1:3, score = c(10, 20, 30), stringsAsFactors = FALSE)
  create_table(initial_data, "test_show_inferred")
  
  # Use show_ducklake_query without explicit table_name
  output <- capture.output({
    get_ducklake_table("test_show_inferred") |>
      dplyr::mutate(score = score * 2) |>
      show_ducklake_query()  # table_name inferred
  })
  
  # Should show SQL preview
  expect_true(any(grepl("DuckLake SQL", paste(output, collapse = " "))))
  
  cleanup_temp_ducklake(lake)
})

test_that("show_ducklake_query fails when table_name cannot be determined", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create table
  initial_data <- data.frame(id = 1:3, value = c("a", "b", "c"), stringsAsFactors = FALSE)
  create_table(initial_data, "test_show_no_name")
  
  # Try to use show_ducklake_query without table_name
  conn <- get_ducklake_connection()
  
  expect_error(
    dplyr::tbl(conn, "test_show_no_name") |>
      dplyr::mutate(value = "preview") |>
      show_ducklake_query(),  # No table_name provided
    "table_name must be provided"
  )
  
  cleanup_temp_ducklake(lake)
})

test_that("show_ducklake_query handles filter queries", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("dplyr")
  
  lake <- create_temp_ducklake()
  
  # Create table
  initial_data <- data.frame(
    id = 1:5,
    status = c("active", "inactive", "active", "inactive", "active"),
    stringsAsFactors = FALSE
  )
  create_table(initial_data, "test_show_filter")
  
  # Preview filtered operation
  output <- capture.output({
    get_ducklake_table("test_show_filter") |>
      dplyr::filter(status == "inactive") |>
      show_ducklake_query()
  })
  
  # Should show SQL preview
  sql_output <- paste(output, collapse = " ")
  expect_true(any(grepl("DuckLake SQL", sql_output)))
  
  cleanup_temp_ducklake(lake)
})